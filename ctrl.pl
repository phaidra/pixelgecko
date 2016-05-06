#!/usr/bin/perl

use strict;

my $REDIS_HOST= 'localhost';
my $REDIS_PORT= 6379;
my $REDIS_DATABASE= 0;

my $REDIS_broker="--broker=redis://$REDIS_HOST:$REDIS_PORT/$REDIS_DATABASE";

my @start_flower= (qw(flower --port=5555), $REDIS_broker);

print "start_flower=[",join (' ',@start_flower),"]\n";
child ('flower', \@start_flower);

sub child
{
  my $name= shift;
  my $cmd= shift;

  print "forking child=[$name] cmd=[",join (' ',@$cmd),"]\n";

  my $rc= fork();

     if ($rc == 0) { system (@$cmd); }
  elsif ($rc >  0) { print "child forked: pid=[$rc]\n"; }
  else { print "something happend: rc=[$rc]\n"; }

  $rc;
}

