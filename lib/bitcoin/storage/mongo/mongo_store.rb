# encoding: ascii-8bit
Bitcoin.require_dependency :mongo

include Mongo

module Bitcoin::Storage::Backends
  class MongoStore < StoreBase

    #Collection
    BLK = "blk"
    TX = "tx"

    #Block document attributes
    HASH = "hash"
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
      @client.drop_database(@db.name)
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
        WORK => (prev_work + blk.block_work).to_s
      }
      block_document[AUX_POW] = blk.aux_pow.to_payload.blob if blk.aux_pow
      #update or create
      update_response = db[BLK].find_and_modify(:query => {HASH => blk.hash.htb.blob}, :update => block_document, :new => true, :upsert => true, :full_response => true)
      #if its a new block
      unless update_response["lastErrorObject"]["updateExisting"]
        # collect the transactions that are already in the db
        existing_tx = Set.new @db[TX].find(HASH => {"$in" => blk.tx.map {|tx| tx.hash.htb.blob }}).map { |tx| tx[:hash].hth }
        blk.tx.each.with_index do |tx, idx|
          if existing_tx.include? tx.hash
            blk_tx[idx] = existing_tx[tx.hash]
          else
            new_tx << [tx, idx]
          end
        end

          #new_tx_ids = fast_insert(:tx, new_tx.map {|tx, _| tx_data(tx) }, return_ids: true)
          #new_tx_ids.each.with_index {|tx_id, idx| blk_tx[new_tx[idx][1]] = tx_id }

          #fast_insert(:blk_tx, blk_tx.map.with_index {|id, idx| { blk_id: block_id, tx_id: id, idx: idx } })

      end

    end

    # sequel:
    def persist_block blk, chain, depth, prev_work = 0
      @db.transaction do
        attrs = {
          :hash => blk.hash.htb.blob,
          :depth => depth,
          :chain => chain,
          :version => blk.ver,
          :prev_hash => blk.prev_block.reverse.blob,
          :mrkl_root => blk.mrkl_root.reverse.blob,
          :time => blk.time,
          :bits => blk.bits,
          :nonce => blk.nonce,
          :blk_size => blk.to_payload.bytesize,
          :work => (prev_work + blk.block_work).to_s
        }
        attrs[:aux_pow] = blk.aux_pow.to_payload.blob  if blk.aux_pow
        existing = @db[:blk].filter(:hash => blk.hash.htb.blob)
        if existing.any?
          existing.update attrs
          block_id = existing.first[:id]
        else
          block_id = @db[:blk].insert(attrs)
          blk_tx, new_tx, addrs, names = [], [], [], []

          # store tx
          existing_tx = Hash[*@db[:tx].filter(hash: blk.tx.map {|tx| tx.hash.htb.blob }).map { |tx| [tx[HASH].hth, tx[:id]] }.flatten]
          blk.tx.each.with_index do |tx, idx|
            existing = existing_tx[tx.hash]
            existing ? blk_tx[idx] = existing : new_tx << [tx, idx]
          end

          new_tx_ids = fast_insert(:tx, new_tx.map {|tx, _| tx_data(tx) }, return_ids: true)
          new_tx_ids.each.with_index {|tx_id, idx| blk_tx[new_tx[idx][1]] = tx_id }

          fast_insert(:blk_tx, blk_tx.map.with_index {|id, idx| { blk_id: block_id, tx_id: id, idx: idx } })

          # store txins
          fast_insert(:txin, new_tx.map.with_index {|tx, tx_idx|
            tx, _ = *tx
            tx.in.map.with_index {|txin, txin_idx|
              txin_data(new_tx_ids[tx_idx], txin, txin_idx) } }.flatten)

          # store txouts
          txout_i = 0
          txout_ids = fast_insert(:txout, new_tx.map.with_index {|tx, tx_idx|
            tx, _ = *tx
            tx.out.map.with_index {|txout, txout_idx|
              script_type, a, n = *parse_script(txout, txout_i, tx.hash, txout_idx)
              addrs += a; names += n; txout_i += 1
              txout_data(new_tx_ids[tx_idx], txout, txout_idx, script_type) } }.flatten, return_ids: true)

          # store addrs
          persist_addrs addrs.map {|i, h| [txout_ids[i], h]}
          names.each {|i, script| store_name(script, txout_ids[i]) }
        end
        @head = wrap_block(attrs.merge(id: block_id))  if chain == MAIN
        @db[:blk].where(:prev_hash => blk.hash.htb.blob, :chain => ORPHAN).each do |b|
          log.debug { "connecting orphan #{b[:hash].hth}" }
          begin
            store_block(get_block(b[:hash].hth))
          rescue SystemStackError
            EM.defer { store_block(get_block(b[:hash].hth)) }  if EM.reactor_running?
          end
        end
        return depth, chain
      end
    end


    #dummy:
    def persist_block(blk, chain, depth, prev_work = 0)
      return [depth, chain]  unless blk && chain == 0
      if block = get_block(blk.hash)
        log.info { "Block already stored; skipping" }
        return false
      end

      blk.tx.each {|tx| store_tx(tx) }
      @blk << blk

      log.info { "NEW HEAD: #{blk.hash} DEPTH: #{get_depth}" }
      [depth, chain]
    end


    # DUMMY:


    def store_tx(tx, validate = true)
      if @tx.keys.include?(tx.hash)
        log.info { "Tx already stored; skipping" }
        return tx
      end
      @tx[tx.hash] = tx
    end

    def has_block(blk_hash)
      !!get_block(blk_hash)
    end

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

    def get_block(blk_hash)
      wrap_block(@blk.find {|blk| blk.hash == blk_hash})
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

    def wrap_block(block)
      return nil  unless block
      data = { id: @blk.index(block), depth: @blk.index(block),
        work: @blk.index(block), chain: MAIN, size: block.size }
      blk = Bitcoin::Storage::Models::Block.new(self, data)
      [:ver, :prev_block, :mrkl_root, :time, :bits, :nonce].each do |attr|
        blk.send("#{attr}=", block.send(attr))
      end
      block.tx.each do |tx|
        blk.tx << get_tx(tx.hash)
      end
      blk.recalc_block_hash
      blk
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