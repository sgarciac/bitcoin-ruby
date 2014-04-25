# encoding: ascii-8bit
Bitcoin.require_dependency :mongo

include Mongo

module Bitcoin::Storage::Backends

  # A dummy implementation based on mongodb
  class MongoStore < StoreBase

    #Collection
    BLK = "blk"
    TX = "tx"

    #attributes
    HASH = "hash"
    TX = "tx"
    DEPTH = "depth"
    CHAIN = "chain"
    VERSION = "version"
    PREV_HASH = "prev_hash"
    MRKL_ROOT = "mrkl_root"
    TIME = "time"
    BITS = "bits"
    NONCE = "nonce"
    BLK_SIZE = "blk_size"
    WORK = "work"
    AUX_POW = "aux_pow"
    COINBASE = "coinbase"
    TX_SIZE = "tx_size"
    BLK_HASH = "blk_hash"

    attr_accessor :db, :client

    DEFAULT_CONFIG = {
      db: "mongodb://localhost/bitcoin"
    }

    # create mongo store with given +config+
    def initialize config, *args
      super config, *args
    end

    def init_store_connection
      return unless @config[:db]
      log.info { "Connecting to #{@config[:db]}" }
      @client = MongoClient.from_uri(@config[:db])
      @db = @client.db
      log.info { @db.name }
    end

    def reset
      return unless @client && @db
      database_name = @db.name
      @client.drop_database(database_name)
      @db = @client.db(database_name)
    end

    #mongo
    def persist_block blk, chain, depth, prev_work = 0
      block_document = {
        HASH => blk.hash.htb.blob,
        DEPTH => depth,
        CHAIN => chain,
        VERSION => blk.ver,
        PREV_HASH => blk.prev_block.reverse.blob,
        MRKL_ROOT => blk.mrkl_root.reverse.blob,
        TIME => blk.time,
        BITS => blk.bits,
        NONCE => blk.nonce,
        BLK_SIZE => blk.to_payload.bytesize,
        TX => blk.tx.map {|tx| tx.hash.htb.blob},
        WORK => (prev_work + blk.block_work).to_s
      }
      block_document[AUX_POW] = blk.aux_pow.to_payload.blob if blk.aux_pow
      # create or update
      db[BLK].update({HASH => blk.hash.htb.blob}, block_document, { :upsert => true })
      blk.tx.each {|tx| store_tx(tx) }
      log.info { "NEW HEAD: #{blk.hash} DEPTH: #{get_depth}" }
      [depth, chain]
    end

    # store transaction +tx+
    def store_tx(tx, validate = true)
      @log.debug { "Storing tx #{tx.hash} (#{tx.to_payload.bytesize} bytes)" }
      tx.validator(self).validate(raise_errors: true)  if validate
      transaction = @db[TX].find_one(HASH => tx.hash.htb.blob)
      return transaction[:id]  if transaction
      @db[TX].update({HASH => tx.hash.htb.blob},tx_data(tx), {:upsert => true})
      tx.in.each_with_index {|i, idx| store_txin(tx_id, i, idx)}
      tx.out.each_with_index {|o, idx| store_txout(tx_id, o, idx, tx.hash)}
      tx.hash.htb.blob
    end

    # prepare tx data for storage in mongo
    def tx_data tx
      data = {
        HASH => tx.hash.htb.blob,
        VERSION => tx.ver, lock_time: tx.lock_time,
        COINBASE => tx.in.size == 1 && tx.in[0].coinbase?,
        TX_SIZE => tx.payload.bytesize,
      }
      data[:nhash] = tx.nhash.htb.blob  if @config[:index_nhash]
      data
    end

    # wrap given +block+ into Models::Block
    def wrap_block(block)
      return nil  unless block
      data = {:id => block[HASH], :depth => block[DEPTH], :chain => block[CHAIN], :work => block[WORK].to_i, :hash => block[HASH].hth, :size => block[BLK_SIZE]}
      blk = Bitcoin::Storage::Models::Block.new(self, data)
      blk.ver = block[VERSION]
      blk.prev_block = block[PREV_HASH].reverse
      blk.mrkl_root = block[MRKL_ROOT].reverse
      blk.time = block[TIME].to_i
      blk.bits = block[BITS]
      blk.nonce = block[NONCE]

      blk.aux_pow = Bitcoin::P::AuxPow.new(block[AUX_POW])  if block[AUX_POW]

      #here: recover transactions from db

      blk_tx = db[:blk_tx].filter(blk_id: block[:id]).join(:tx, id: :tx_id).order(:idx)

      # fetch inputs and outputs for all transactions in the block to avoid additional queries for each transaction
      inputs = db[:txin].filter(:tx_id => blk_tx.map{ |tx| tx[:id] }).order(:tx_idx).map.group_by{ |txin| txin[:tx_id] }
      outputs = db[:txout].filter(:tx_id => blk_tx.map{ |tx| tx[:id] }).order(:tx_idx).map.group_by{ |txout| txout[:tx_id] }

      blk.tx = blk_tx.map { |tx| wrap_tx(tx, block[:id], inputs: inputs[tx[:id]], outputs: outputs[tx[:id]]) }

      blk.hash = block[:hash].hth
      blk
    end



    def get_block(blk_hash)
      wrap_block(@blk.find_one(HASH => blk_hash))
    end

    def has_block(blk_hash)
      !!get_block(blk_hash)
    end


    # sequel:


    # DUMMY:




    def has_tx(tx_hash)
      !!get_tx(tx_hash)
    end

    def get_depth
      @blk.size - 1
    end

    def get_head
      wrap_block(@blk[-1])
    end

    def get_block_by_depth(depth)
      wrap_block(@blk[depth])
    end

    def get_block_by_prev_hash(hash)
      wrap_block(@blk.find {|blk| blk.prev_block == [hash].pack("H*").reverse})
    end


    def get_block_by_id(blk_id)
      wrap_block(@blk[blk_id])
    end

    def get_block_by_tx(tx_hash)
      wrap_block(@blk.find {|blk| blk.tx.map(&:hash).include?(tx_hash) })
    end

    def get_idx_from_tx_hash(tx_hash)
      return nil unless tx = get_tx(tx_hash)
      return nil unless blk = tx.get_block
      blk.tx.index tx
    end

    def get_tx(tx_hash)
      transaction = @tx[tx_hash]
      return nil  unless transaction
      wrap_tx(transaction)
    end

    def get_tx_by_id(tx_id)
      wrap_tx(@tx[tx_id])
    end

    def get_txin_for_txout(tx_hash, txout_idx)
      txin = @tx.values.map(&:in).flatten.find {|i| i.prev_out_index == txout_idx &&
        i.prev_out == [tx_hash].pack("H*").reverse }
      wrap_txin(txin)
    end

    def get_txout_for_txin(txin)
      return nil unless tx = @tx[txin.prev_out.reverse_hth]
      wrap_tx(tx).out[txin.prev_out_index]
    end

    def get_txouts_for_pk_script(script)
      txouts = @tx.values.map(&:out).flatten.select {|o| o.pk_script == script}
      txouts.map {|o| wrap_txout(o) }
    end

    def get_txouts_for_hash160(hash160, unconfirmed = false)
      @tx.values.map(&:out).flatten.map {|o|
        o = wrap_txout(o)
        if o.parsed_script.is_multisig?
          o.parsed_script.get_multisig_pubkeys.map{|pk| Bitcoin.hash160(pk.unpack("H*")[0])}.include?(hash160) ? o : nil
        else
          o.hash160 == hash160 ? o : nil
        end
      }.compact
    end


    def wrap_tx(transaction)
      return nil  unless transaction
      blk = @blk.find{|b| b.tx.include?(transaction)}
      data = { id: transaction.hash, blk_id: @blk.index(blk),
        size: transaction.size }
      tx = Bitcoin::Storage::Models::Tx.new(self, data)
      tx.ver = transaction.ver
      tx.lock_time = transaction.lock_time
      transaction.in.each {|i| tx.add_in(wrap_txin(i))}
      transaction.out.each {|o| tx.add_out(wrap_txout(o))}
      tx.hash = tx.hash_from_payload(tx.to_payload)
      tx
    end

    def wrap_txin(input)
      return nil  unless input
      tx = @tx.values.find{|t| t.in.include?(input)}
      data = { tx_id: tx.hash, tx_idx: tx.in.index(input)}
      txin = Bitcoin::Storage::Models::TxIn.new(self, data)
      [:prev_out, :prev_out_index, :script_sig_length, :script_sig, :sequence].each do |attr|
        txin.send("#{attr}=", input.send(attr))
      end
      txin
    end

    def wrap_txout(output)
      return nil  unless output
      tx = @tx.values.find{|t| t.out.include?(output)}
      data = {tx_id: tx.hash, tx_idx: tx.out.index(output), hash160: output.parsed_script.get_hash160}
      txout = Bitcoin::Storage::Models::TxOut.new(self, data)
      [:value, :pk_script_length, :pk_script].each do |attr|
        txout.send("#{attr}=", output.send(attr))
      end
      txout
    end

    def check_consistency(*args)
      log.warn { "MongoDB store doesn't support consistency check" }
    end

    def to_s
      "DummyStore"
    end

  end
end
