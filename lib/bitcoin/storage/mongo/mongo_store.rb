# encoding: ascii-8bit

Bitcoin.require_dependency :mongo

include Mongo

class String; def blob; BSON::Binary.new(self); end; end

module Bitcoin::Storage::Backends

  # A dumb implementation based on mongodb.
  class MongoStore < StoreBase

    #Collections
    BLK = "blk"
    TX = "tx"
    TXIN = "txin"
    TXOUT = "txout"

    #Attributes
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
    COINBASE = "coinbase"
    TX_SIZE = "tx_size"
    NHASH = "nhash"
    BLK_HASH = "blk_hash"
    TX_HASH = "tx_hash"
    LOCK_TIME = "lock_time"
    IDX = "idx"
    PREV_OUT = "prev_out"
    PREV_OUT_IDX = "prev_out_idx"
    SCRIPT_SIG = "script_sig"
    SEQUENCE = "sequence"
    HASH160 = "hash160"
    STYPE = "stype"
    VALUE = "value"
    PK_SCRIPT = "pk_script"
    ADDRESSES = "addresses"

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
      @db[BLK].remove
      @db[TX].remove
      @db[TXIN].remove
      @db[TXOUT].remove
      @head = nil
    end

    #mongo
    def persist_block blk, chain, depth, prev_work = 0
      return [depth, chain]  unless blk && chain == MAIN
      block_document = {
        HASH => blk.hash,
        DEPTH => depth,
        CHAIN => chain,
        VERSION => blk.ver,
        PREV_HASH => blk.prev_block.reverse.blob,
        MRKL_ROOT => blk.mrkl_root.reverse.blob, #
        TIME => blk.time,
        BITS => blk.bits,
        NONCE => blk.nonce,
        BLK_SIZE => blk.to_payload.bytesize,
        TX => blk.tx.map {|tx| tx.hash},
        WORK => (prev_work + blk.block_work)
      }
      block_document[AUX_POW] = blk.aux_pow.to_payload.blob if blk.aux_pow
      # create or update
      db[BLK].update({HASH => blk.hash}, block_document, { :upsert => true })
      db[TX].remove({BLK_HASH => blk.hash})
      blk.tx.each_with_index {|tx, idx| store_tx(tx, false, blk.hash, idx) }
      @head = wrap_block block_document
      log.info { "NEW HEAD: #{blk.hash} DEPTH: #{get_depth}" }
      [depth, chain]
    end

    # store transaction +tx+
    def store_tx(tx, validate = true, blk_hash = nil, tx_idx = 0)
      @log.debug { "Storing tx #{tx.hash} (#{tx.to_payload.bytesize} bytes)" }
      tx.validator(self).validate(raise_errors: true)  if validate # TODO: check this
      @db[TX].update({HASH => tx.hash},tx_data(tx, blk_hash, tx_idx), {:upsert => true})
      tx.in.each_with_index {|i, idx| store_txin(tx.hash, i, idx)}
      tx.out.each_with_index {|o, idx| store_txout(tx.hash, o, idx, tx.hash)}
      tx.hash
    end

    # prepare tx data for storage in mongo
    def tx_data tx, blk_hash = nil, tx_idx = 0
      data = {
        HASH => tx.hash,
        VERSION => tx.ver, lock_time: tx.lock_time,
        COINBASE => tx.in.size == 1 && tx.in[0].coinbase?,
        TX_SIZE => tx.payload.bytesize,
        IDX => tx_idx
      }
      data[NHASH] = tx.nhash if @config[:index_nhash]
      data[BLK_HASH] = blk_hash if blk_hash
      data
    end

    # wrap given +block+ into Models::Block
    def wrap_block(block)
      return nil unless block
      data = {:id => block[HASH],
        :depth => block[DEPTH],
        :chain => block[CHAIN],
        :work => block[WORK].to_i,
        :hash => block[HASH],
        :size => block[BLK_SIZE]}
      blk = Bitcoin::Storage::Models::Block.new(self, data)
      blk.ver = block[VERSION]
      blk.prev_block = block[PREV_HASH].to_s.reverse
      blk.mrkl_root = block[MRKL_ROOT].to_s.reverse
      blk.time = block[TIME].to_i
      blk.bits = block[BITS]
      blk.nonce = block[NONCE]
      blk.aux_pow = Bitcoin::P::AuxPow.new(block[AUX_POW])  if block[AUX_POW]
      blk.tx = @db[TX].find({BLK_HASH => block[HASH]},
                            :sort => {IDX => Mongo::ASCENDING}).map { |transaction| wrap_tx(transaction) }
      blk.hash = block[HASH]
      blk
    end

    def get_block(blk_hash)
      wrap_block(@db[BLK].find_one(HASH => blk_hash))
    end

    def has_block(blk_hash)
      get_block(blk_hash)
    end

    def has_tx(tx_hash)
      get_tx(tx_hash)
    end

    # Get the depth of the main chain
    def get_depth
      depth = (@config[:cache_head] && @head) ? @head.depth :
        @depth = @db[BLK].find({CHAIN => MAIN}, :sort => {DEPTH => Mongo::DESCENDING}, :limit=>1).first[DEPTH] rescue nil
      depth ? depth : -1
    end

    # get head block (highest block from the MAIN chain)
    def get_head
      (@config[:cache_head] && @head) ? @head :
        @head = wrap_block(@db[BLK].find({CHAIN => MAIN},:sort => {DEPTH => Mongo::DESCENDING}, :limit => 1).first)
    end


    def get_block_by_depth(depth)
      wrap_block(@db[BLK].find_one(DEPTH => depth, CHAIN => MAIN))
    end

    # get block by given +prev_hash+
    def get_block_by_prev_hash(prev_hash)
      wrap_block(@db[BLK].find_one(PREV_HASH => prev_hash.blob, CHAIN => MAIN))
    end

      # get block by given +tx_hash+
    def get_block_by_tx(tx_hash)
      wrap_block(@db[BLK].find_one(TX => tx_hash))
    end

    # get block by given +id+
    def get_block_by_id(block_id)
      get_block(block_id)
    end

    # Grab the position of a tx in a given block
    def get_idx_from_tx_hash(tx_hash)
      transaction = @db[TX].find_one(HASH => tx_hash)
      return nil unless transaction
      transaction[IDX]
    end

    # get transaction for given +tx_hash+
    def get_tx(tx_hash)
      wrap_tx(@db[TX].find_one(HASH => tx_hash))
    end

    def wrap_tx(transaction)
      return nil  unless transaction
      data = {id: transaction[HASH], blk_id: transaction[BLK_HASH], size: transaction[TX_SIZE], idx: transaction[IDX]}
      tx = Bitcoin::Storage::Models::Tx.new(self, data)

      inputs = db[TXIN].find({TX_HASH => transaction[HASH]}, :sort => {IDX => 1})
      inputs.each { |i| tx.add_in(wrap_txin(i)) }

      outputs = db[TXOUT].find({TX_HASH => transaction[HASH]}, :sort => {IDX => 1})
      outputs.each { |o| tx.add_out(wrap_txout(o)) }

      tx.ver = transaction[VERSION]
      tx.lock_time = transaction[LOCK_TIME]
      tx.hash = transaction[HASH]
      tx
    end

    def get_tx_by_id(tx_id)
      get_tx(tx_id)
    end

    def get_txin_for_txout(tx_hash, txout_idx)
      tx_hash = tx_hash.htb_reverse
      wrap_txin(@db[TXIN].find_one(PREV_OUT => tx_hash, PREV_OUT_IDX => txout_idx))
    end

    def get_txout_for_txin(txin)
      tx = @db[TX].find_one(HASH => txin.prev_out)
      return nil unless tx
      wrap_txout(@db[TXOUT].find_one(IDX => txin.prev_out_index, TX_HASH => tx[HASH]))
    end

    # wrap given +input+ into Models::TxIn
    def wrap_txin(input)
      return nil unless input
      data = {:id => input["_id"], :tx_id => input[TX_HASH], :tx_idx => input[IDX]}
      txin = Bitcoin::Storage::Models::TxIn.new(self, data)
      txin.prev_out = input[PREV_OUT]
      txin.prev_out_index = input[PREV_OUT_IDX]
      txin.script_sig_length = input[SCRIPT_SIG].to_s.bytesize
      txin.script_sig = input[SCRIPT_SIG].to_s
      txin.sequence = [input[SEQUENCE]].pack("V")
      txin
    end

    # wrap given +output+ into Models::TxOut
    def wrap_txout(output)
      return nil unless output
      data = {:id => output["_id"], :tx_id => output[TX_HASH], :tx_idx => output[IDX],
        :hash160 => output[HASH160], :type => SCRIPT_TYPES[output[STYPE]]}
      txout = Bitcoin::Storage::Models::TxOut.new(self, data)
      txout.value = output[VALUE]
      txout.pk_script = output[PK_SCRIPT].to_s
      txout
    end

    def store_txin(tx_id, txin, idx)
      @db[TXIN].insert(txin_data(tx_id, txin, idx))
    end


    # prepare txin data for storage
    def txin_data tx_id, txin, idx
      { TX_HASH => tx_id,
        IDX => idx,
        SCRIPT_SIG => txin.script_sig.blob,
        PREV_OUT => txin.prev_out.blob,
        PREV_OUT_IDX => txin.prev_out_index,
        SEQUENCE => txin.sequence.unpack("V")[0] }
    end


        # store output +txout+
    def store_txout(tx_id, txout, idx, tx_hash = "")
      script_type, addrs, names = *parse_script(txout, idx, tx_hash, idx)
      txout_id = @db[TXOUT].insert(txout_data(tx_id, txout, idx, script_type, addrs.map {|i, h| h}))

      #persist_addrs addrs.map {|i, h| [txout_id, h] }
      #names.each {|i, script| store_name(script, txout_id) }
      txout_id
    end


    # prepare txout data for storage
    def txout_data tx_id, txout, idx, script_type, addrs = []
      { TX_HASH => tx_id, IDX => idx,
        PK_SCRIPT => txout.pk_script.blob,
        VALUE => txout.value, STYPE => script_type,
        ADDRESSES => addrs
      }
    end


    # get all Models::TxOut matching given +script+
    def get_txouts_for_pk_script(script)
      txouts = @db[TXOUT].find(PK_SCRIPT => script.blob)
      txouts.map{|txout| wrap_txout(txout)}
    end

    # get all Models::TxOut matching given +hash160+
    def get_txouts_for_hash160(hash160, unconfirmed = false)

    end

    def check_consistency(*args)
      log.warn { "MongoDB store doesn't support consistency check" }
    end

    def reorg(*args)
      log.warn {"MongoDB sotre doesn't support reorg"}
    end

    def to_s
      "MongoStore"
    end

  end
end
