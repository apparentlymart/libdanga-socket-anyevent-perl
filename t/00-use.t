#!/usr/bin/perl -w

use strict;
use Test::More tests => 1;

my $mod = "Danga::Socket::AnyEvent";

use_ok($mod);
