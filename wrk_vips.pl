#!/usr/bin/perl

use strict;

use Data::Dumper;
$Data::Dumper::Indent= 1;
# use MongoDB;
use lib '/home/pige/work/phaidra/forge/perl';
use lib '/home/pige/work/sf/aix-pm/modules/util/';
use PAF::JobQueue;
use Util::JSON;

my $fnm_config= 'pige_conf.json';
my $op_mode;
my $sleep_time= 60;

my @JOBS;
while (defined (my $arg= shift (@ARGV)))
{
  if ($arg eq '--') { push (@JOBS, @ARGV); @ARGV= (); }
  elsif ($arg =~ /^--(.+)/)
  {
    my ($opt, $val)= split ('=', $1, 2);

       if ($opt eq 'direct') { $op_mode= 'direct'; }
    elsif ($opt eq 'watch') { $op_mode= 'watch'; }
    elsif ($opt eq 'config') { $fnm_config= $val||shift (@ARGV); }
  }
  elsif ($arg =~ /^-(.+)/)
  {
  }
  else { push (@JOBS, $arg); }
}

my $config= Util::JSON::read_json_file ($fnm_config);
# print "config: ", Dumper ($config);

if ($op_mode eq 'direct')
{
  while (my $pid= shift (@JOBS))
  {
    my $rc= process_image ($pid);
  }
  exit (0);
}
elsif ($op_mode eq 'watch') {}
else
{
  #usage();
  exit(0);
}

my $jq= new PAF::JobQueue( mongodb => $config->{mongodb} ); #, col => $config->{job_queue} );

# print "jq: ", Dumper ($jq);
# my $x1= $jq->connect();
# print "x1=[$x1]\n";

process_job_queue($jq);

exit (0);

sub process_job_queue
{
  my $jq= shift;

  JOB: while (1)
  {
    my $job= $jq->get_job ( 'pige' );
    unless (defined ($job))
    {
      print "nothing to do, sleeping until ", scalar localtime(time()+ $sleep_time), "\n";
      sleep($sleep_time);
      next JOB;
    }
    print "job: ", Dumper ($job);

    my $rc= process_image ($job->{pid}, $job->{idhash});

    if (!defined ($rc))
    {
      $job->{'status'}= 'failed';
    }
    else
    {
      $job->{'status'}= 'finished';
      foreach my $an (keys %$rc) { $job->{$an}= $rc->{$an} }
    }

    $jq->update_job ($job);

    # sleep(5);
  }
}

sub process_image
{
  my $pid= shift;
  my $idhash= shift;

  my $tmp_dir= $config->{temp_path};
  system ('mkdir', '-p', $tmp_dir) unless (-d $tmp_dir);

  # TODO: parametrize....
  my $url= "https://".$config->{fedorabaseurl}."/fedora/get/${pid}/bdef:Content/download";

  my $img_fnm= $pid; $img_fnm=~ s#:#_#g;
  my $out_img;
  if(defined($idhash) && $idhash =~ /\b([a-f0-9]{40})\b/){
    my $lvl1= substr($idhash, 0, 1);
    my $lvl2= substr($idhash, 1, 1);
    my $out_dir= join ('/', $config->{store}, $lvl1, $lvl2);
    system ('mkdir', '-p', $out_dir) unless (-d $out_dir);
    $out_img= join ('/', $out_dir, $idhash.'.tif');
  }else{
    print "idhash[$idhash] is not defined or is not a SHA-1 hash\n";
    $out_img= join ('/', $config->{store}, $img_fnm.'.tif');
  }
  my $tmp_img= join ('/', $tmp_dir, $img_fnm);

  my @curl= (qw(curl -L), $url, '-o', $tmp_img);
  print "curl: [", join (' ', @curl), "]\n";
  my $curl_txt= `@curl 2>&1`;
  print "curl_txt=[$curl_txt]\n";
  my @curl_lines= x_lines ($curl_txt);

  unless (-f $tmp_img)
  {
    print "ATTN: could not retrieve [$url] and save to [$tmp_img]\n";
    return undef;
  }
  my @tmp_st= stat(_);
  # TODO: check ....

  # we have to autorotate image because IIPServer doesn't return EXIF Orientation metadata field (at least for natively JPEG images)
  # See https://libvips.github.io/libvips/API/current/libvips-conversion.html#vips-autorot
  my $rotate_img = $tmp_img.'.v';
  my @rotate= (qw(/usr/bin/vips autorot), $tmp_img, $rotate_img);
  my $rotate= join (' ', @rotate);
  print "rotate: [$rotate]\n";
  my $rotate_txt= `@rotate 2>&1`;
  print "rotate_txt=[$rotate_txt]\n";
  my @rotate_lines= x_lines ($rotate_txt);

  unless (-f $rotate_img)
  {
      print "ATTN: could not save [$rotate_img] using rotate=[$rotate]\n";
      return undef;
  }

  my @vips= (qw(/usr/bin/vips tiffsave --vips-progress --vips-concurrency 4), $tmp_img.'.v', $out_img, qw(--tile --pyramid --compression jpeg --Q 85 --tile-width 256 --tile-height 256));
  my $vips= join (' ', @vips);
  print "vips: [$vips]\n";
  my $vips_txt= `@vips 2>&1`;
  print "vips_txt=[$vips_txt]\n";
  my @vips_lines= x_lines ($vips_txt);

  unless (-f $out_img)
  {
    print "ATTN: could not save [$out_img] using vips=[$vips]\n";
    return undef;
  }
  my @out_st= stat(_);
  # TODO: check ....

  unlink ($rotate_img);
  unlink ($tmp_img);

  return { 'conversion' => 'ok', 'image' => $out_img, vips_lines => \@vips_lines, curl_lines => \@curl_lines, rotate_lines => \@rotate_lines };
}

sub x_lines
{
  my $s= shift;

  my @l= split (/\n/, $s);
  my @l2= ();
  while (my $l= shift (@l))
  {
    $l=~ s#.*\r##g;
    $l=~ s# *$##;
    push (@l2, $l);
  }

  (wantarray) ? @l2 : \@l2;
}

__END__

https://fedora.volare.vorarlberg.at/fedora/get/o:1889/bdef:Content/download

vips im_vips2tiff --vips-progress --vips-concurrency=4  /data/tmp/%s /data/public/%s.tif:deflate,tile:256x256,pyramid"%(oid,oid)

db.jobs.insert({agent:"pige",status:"new",pid:"o:1928"})

