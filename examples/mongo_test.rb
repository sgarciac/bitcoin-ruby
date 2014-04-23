#!/usr/bin/env ruby
#

$:.unshift( File.expand_path("../../lib", __FILE__) )
require 'bitcoin'

Bitcoin.network = :bitcoin
store = Bitcoin::Storage.mongo(:db => "mongodb://localhost/bitcoin")
