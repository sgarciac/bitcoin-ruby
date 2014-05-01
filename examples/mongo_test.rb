#!/usr/bin/env ruby
#

$:.unshift( File.expand_path("../../lib", __FILE__) )
require 'bitcoin'

Bitcoin.network = :bitcoin
#use a default url
store = Bitcoin::Storage.mongo(:db => "mongodb://localhost/bitcoin")
puts store.get_depth
