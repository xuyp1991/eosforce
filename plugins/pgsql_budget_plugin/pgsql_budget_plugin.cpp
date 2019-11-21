/**
 *  @file
 *  @copyright defined in eos/LICENSE
 */
#include <eosio/pgsql_budget_plugin/pgsql_budget_plugin.hpp>
#include <eosio/chain/eosio_contract.hpp>
#include <eosio/chain/config.hpp>
#include <eosio/chain/exceptions.hpp>
#include <eosio/chain/transaction.hpp>
#include <eosio/chain/types.hpp>

#include <fc/io/json.hpp>
#include <fc/log/logger_config.hpp>
#include <fc/utf8.hpp>
#include <fc/variant.hpp>

#include <boost/algorithm/string.hpp>
#include <boost/chrono.hpp>
#include <boost/signals2/connection.hpp>

#include <queue>
#include <thread>
#include <mutex>

#include <eosio/chain/genesis_state.hpp>

namespace fc { class variant; }

namespace eosio {

using chain::account_name;
using chain::action_name;
using chain::block_id_type;
using chain::permission_name;
using chain::transaction;
using chain::signed_transaction;
using chain::signed_block;
using chain::transaction_id_type;
using chain::packed_transaction;

static appbase::abstract_plugin& _mongo_db_plugin = app().register_plugin<pgsql_budget_plugin>();


class pgsql_budget_plugin_impl {
   public:
      pgsql_budget_plugin_impl();
      ~pgsql_budget_plugin_impl();

      fc::optional<boost::signals2::scoped_connection> accepted_block_connection;
      fc::optional<boost::signals2::scoped_connection> irreversible_block_connection;
      fc::optional<boost::signals2::scoped_connection> accepted_transaction_connection;
      fc::optional<boost::signals2::scoped_connection> applied_transaction_connection;

      void consume_blocks();

      void accepted_block( const chain::block_state_ptr& );
      void applied_irreversible_block(const chain::block_state_ptr&);
      void accepted_transaction(const chain::transaction_metadata_ptr&);
      void applied_transaction(const chain::transaction_trace_ptr&);
      void process_accepted_transaction(const chain::transaction_metadata_ptr&);
      void _process_accepted_transaction(const chain::transaction_metadata_ptr&);
      void process_applied_transaction(const chain::transaction_trace_ptr&);
      void _process_applied_transaction(const chain::transaction_trace_ptr&);
      void process_accepted_block( const chain::block_state_ptr& );
      void _process_accepted_block( const chain::block_state_ptr& );
      void process_irreversible_block(const chain::block_state_ptr&);
      void _process_irreversible_block(const chain::block_state_ptr&);

      

      void init();


      template<typename Queue, typename Entry> void queue(Queue& queue, const Entry& e);

      bool configured{false};
      bool wipe_database_on_startup{false};
      uint32_t start_block_num = 0;
      std::atomic_bool start_block_reached{false};

      bool is_producer = false;

      bool update_blocks_via_block_num = false;
      bool store_blocks = true;
      bool store_block_states = true;
      bool store_transactions = true;
      bool store_transaction_traces = true;
      bool store_action_traces = true;
      uint32_t expire_after_seconds = 0;

      std::string db_name;


      size_t max_queue_size = 0;
      int queue_sleep_time = 0;
      size_t abi_cache_size = 0;
      std::deque<chain::transaction_metadata_ptr> transaction_metadata_queue;
      std::deque<chain::transaction_metadata_ptr> transaction_metadata_process_queue;
      std::deque<chain::transaction_trace_ptr> transaction_trace_queue;
      std::deque<chain::transaction_trace_ptr> transaction_trace_process_queue;
      std::deque<chain::block_state_ptr> block_state_queue;
      std::deque<chain::block_state_ptr> block_state_process_queue;
      std::deque<chain::block_state_ptr> irreversible_block_state_queue;
      std::deque<chain::block_state_ptr> irreversible_block_state_process_queue;
      std::mutex mtx;
      std::condition_variable condition;
      std::thread consume_thread;
      std::atomic_bool done{false};
      std::atomic_bool startup{true};
      fc::optional<chain::chain_id_type> chain_id;
      fc::microseconds abi_serializer_max_time;

};

template<typename Queue, typename Entry>
void pgsql_budget_plugin_impl::queue( Queue& queue, const Entry& e ) {
   std::unique_lock<std::mutex> lock( mtx );
   auto queue_size = queue.size();
   if( queue_size > max_queue_size ) {
      lock.unlock();
      condition.notify_one();
      queue_sleep_time += 10;
      if( queue_sleep_time > 1000 )
         wlog("queue size: ${q}", ("q", queue_size));
      std::this_thread::sleep_for( std::chrono::milliseconds( queue_sleep_time ));
      lock.lock();
   } else {
      queue_sleep_time -= 10;
      if( queue_sleep_time < 0 ) queue_sleep_time = 0;
   }
   queue.emplace_back( e );
   lock.unlock();
   condition.notify_one();
}

void pgsql_budget_plugin_impl::accepted_transaction( const chain::transaction_metadata_ptr& t ) {
   try {
      if( store_transactions ) {
         queue( transaction_metadata_queue, t );
      }
   } catch (fc::exception& e) {
      elog("FC Exception while accepted_transaction ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while accepted_transaction ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while accepted_transaction");
   }
}

void pgsql_budget_plugin_impl::applied_transaction( const chain::transaction_trace_ptr& t ) {
   try {
      if( !is_producer && !t->producer_block_id.valid() )
         return;
      // always queue since account information always gathered
      queue( transaction_trace_queue, t );
   } catch (fc::exception& e) {
      elog("FC Exception while applied_transaction ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while applied_transaction ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while applied_transaction");
   }
}

void pgsql_budget_plugin_impl::applied_irreversible_block( const chain::block_state_ptr& bs ) {
   try {
      if( store_blocks || store_block_states || store_transactions ) {
         queue( irreversible_block_state_queue, bs );
      }
   } catch (fc::exception& e) {
      elog("FC Exception while applied_irreversible_block ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while applied_irreversible_block ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while applied_irreversible_block");
   }
}

void pgsql_budget_plugin_impl::accepted_block( const chain::block_state_ptr& bs ) {
   try {
      if( !start_block_reached ) {
         if( bs->block_num >= start_block_num ) {
            start_block_reached = true;
         }
      }
      if( store_blocks || store_block_states ) {
         queue( block_state_queue, bs );
      }
   } catch (fc::exception& e) {
      elog("FC Exception while accepted_block ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while accepted_block ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while accepted_block");
   }
}

void pgsql_budget_plugin_impl::consume_blocks() {
   try {

      while (true) {
         std::unique_lock<std::mutex> lock(mtx);
         while ( transaction_metadata_queue.empty() &&
                 transaction_trace_queue.empty() &&
                 block_state_queue.empty() &&
                 irreversible_block_state_queue.empty() &&
                 !done ) {
            condition.wait(lock);
         }

         // capture for processing
         size_t transaction_metadata_size = transaction_metadata_queue.size();
         if (transaction_metadata_size > 0) {
            transaction_metadata_process_queue = move(transaction_metadata_queue);
            transaction_metadata_queue.clear();
         }
         size_t transaction_trace_size = transaction_trace_queue.size();
         if (transaction_trace_size > 0) {
            transaction_trace_process_queue = move(transaction_trace_queue);
            transaction_trace_queue.clear();
         }
         size_t block_state_size = block_state_queue.size();
         if (block_state_size > 0) {
            block_state_process_queue = move(block_state_queue);
            block_state_queue.clear();
         }
         size_t irreversible_block_size = irreversible_block_state_queue.size();
         if (irreversible_block_size > 0) {
            irreversible_block_state_process_queue = move(irreversible_block_state_queue);
            irreversible_block_state_queue.clear();
         }

         lock.unlock();

         if (done) {
            ilog("draining queue, size: ${q}", ("q", transaction_metadata_size + transaction_trace_size + block_state_size + irreversible_block_size));
         }

         // process transactions
         auto start_time = fc::time_point::now();
         auto size = transaction_trace_process_queue.size();
         while (!transaction_trace_process_queue.empty()) {
            const auto& t = transaction_trace_process_queue.front();
            process_applied_transaction(t);
            transaction_trace_process_queue.pop_front();
         }
         auto time = fc::time_point::now() - start_time;
         auto per = size > 0 ? time.count()/size : 0;
         if( time > fc::microseconds(500000) ) // reduce logging, .5 secs
            ilog( "process_applied_transaction,  time per: ${p}, size: ${s}, time: ${t}", ("s", size)("t", time)("p", per) );

         start_time = fc::time_point::now();
         size = transaction_metadata_process_queue.size();
         while (!transaction_metadata_process_queue.empty()) {
            const auto& t = transaction_metadata_process_queue.front();
            process_accepted_transaction(t);
            transaction_metadata_process_queue.pop_front();
         }
         time = fc::time_point::now() - start_time;
         per = size > 0 ? time.count()/size : 0;
         if( time > fc::microseconds(500000) ) // reduce logging, .5 secs
            ilog( "process_accepted_transaction, time per: ${p}, size: ${s}, time: ${t}", ("s", size)( "t", time )( "p", per ));

         // process blocks
         start_time = fc::time_point::now();
         size = block_state_process_queue.size();
         while (!block_state_process_queue.empty()) {
            const auto& bs = block_state_process_queue.front();
            process_accepted_block( bs );
            block_state_process_queue.pop_front();
         }
         time = fc::time_point::now() - start_time;
         per = size > 0 ? time.count()/size : 0;
         if( time > fc::microseconds(500000) ) // reduce logging, .5 secs
            ilog( "process_accepted_block,       time per: ${p}, size: ${s}, time: ${t}", ("s", size)("t", time)("p", per) );

         // process irreversible blocks
         start_time = fc::time_point::now();
         size = irreversible_block_state_process_queue.size();
         while (!irreversible_block_state_process_queue.empty()) {
            const auto& bs = irreversible_block_state_process_queue.front();
            process_irreversible_block(bs);
            irreversible_block_state_process_queue.pop_front();
         }
         time = fc::time_point::now() - start_time;
         per = size > 0 ? time.count()/size : 0;
         if( time > fc::microseconds(500000) ) // reduce logging, .5 secs
            ilog( "process_irreversible_block,   time per: ${p}, size: ${s}, time: ${t}", ("s", size)("t", time)("p", per) );

         if( transaction_metadata_size == 0 &&
             transaction_trace_size == 0 &&
             block_state_size == 0 &&
             irreversible_block_size == 0 &&
             done ) {
            break;
         }
      }
      ilog("pgsql_budget_plugin consume thread shutdown gracefully");
   } catch (fc::exception& e) {
      elog("FC Exception while consuming block ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while consuming block ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while consuming block");
   }
}



void pgsql_budget_plugin_impl::process_accepted_transaction( const chain::transaction_metadata_ptr& t ) {
   try {
      if( start_block_reached ) {
         _process_accepted_transaction( t );
      }
   } catch (fc::exception& e) {
      elog("FC Exception while processing accepted transaction metadata: ${e}", ("e", e.to_detail_string()));
   } catch (std::exception& e) {
      elog("STD Exception while processing accepted tranasction metadata: ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while processing accepted transaction metadata");
   }
}

void pgsql_budget_plugin_impl::process_applied_transaction( const chain::transaction_trace_ptr& t ) {
   try {
      // always call since we need to capture setabi on accounts even if not storing transaction traces
      _process_applied_transaction( t );
   } catch (fc::exception& e) {
      elog("FC Exception while processing applied transaction trace: ${e}", ("e", e.to_detail_string()));
   } catch (std::exception& e) {
      elog("STD Exception while processing applied transaction trace: ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while processing applied transaction trace");
   }
}

void pgsql_budget_plugin_impl::process_irreversible_block(const chain::block_state_ptr& bs) {
  try {
     if( start_block_reached ) {
        _process_irreversible_block( bs );
     }
  } catch (fc::exception& e) {
     elog("FC Exception while processing irreversible block: ${e}", ("e", e.to_detail_string()));
  } catch (std::exception& e) {
     elog("STD Exception while processing irreversible block: ${e}", ("e", e.what()));
  } catch (...) {
     elog("Unknown exception while processing irreversible block");
  }
}

void pgsql_budget_plugin_impl::process_accepted_block( const chain::block_state_ptr& bs ) {
   try {
      if( start_block_reached ) {
         _process_accepted_block( bs );
      }
   } catch (fc::exception& e) {
      elog("FC Exception while processing accepted block trace ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while processing accepted block trace ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while processing accepted block trace");
   }
}

void pgsql_budget_plugin_impl::_process_accepted_transaction( const chain::transaction_metadata_ptr& t ) {
   

}


void pgsql_budget_plugin_impl::_process_applied_transaction( const chain::transaction_trace_ptr& t ) {
   

}

void pgsql_budget_plugin_impl::_process_accepted_block( const chain::block_state_ptr& bs ) {
   
}

void pgsql_budget_plugin_impl::_process_irreversible_block(const chain::block_state_ptr& bs)
{
   
}


pgsql_budget_plugin_impl::pgsql_budget_plugin_impl()
{
}

pgsql_budget_plugin_impl::~pgsql_budget_plugin_impl() {
   if (!startup) {
      try {
         ilog( "pgsql_budget_plugin shutdown in process please be patient this can take a few minutes" );
         done = true;
         condition.notify_one();

         consume_thread.join();

      } catch( std::exception& e ) {
         elog( "Exception on pgsql_budget_plugin shutdown of consume thread: ${e}", ("e", e.what()));
      }
   }
}


void pgsql_budget_plugin_impl::init() {
   
}

////////////
// pgsql_budget_plugin
////////////

pgsql_budget_plugin::pgsql_budget_plugin()
:my(new pgsql_budget_plugin_impl)
{
}

pgsql_budget_plugin::~pgsql_budget_plugin()
{
}

void pgsql_budget_plugin::set_program_options(options_description& cli, options_description& cfg)
{
   cfg.add_options()
         ("mongodb-queue-size,q", bpo::value<uint32_t>()->default_value(1024),
         "The target queue size between nodeos and MongoDB plugin thread.")
         ("mongodb-abi-cache-size", bpo::value<uint32_t>()->default_value(2048),
          "The maximum size of the abi cache for serializing data.")
         ("mongodb-wipe", bpo::bool_switch()->default_value(false),
         "Required with --replay-blockchain, --hard-replay-blockchain, or --delete-all-blocks to wipe mongo db."
         "This option required to prevent accidental wipe of mongo db.")
         ("mongodb-block-start", bpo::value<uint32_t>()->default_value(0),
         "If specified then only abi data pushed to mongodb until specified block is reached.")
         ("mongodb-uri,m", bpo::value<std::string>(),
         "MongoDB URI connection string, see: https://docs.mongodb.com/master/reference/connection-string/."
               " If not specified then plugin is disabled. Default database 'EOS' is used if not specified in URI."
               " Example: mongodb://127.0.0.1:27017/EOS")
         ("mongodb-update-via-block-num", bpo::value<bool>()->default_value(false),
          "Update blocks/block_state with latest via block number so that duplicates are overwritten.")
         ("mongodb-store-blocks", bpo::value<bool>()->default_value(true),
          "Enables storing blocks in mongodb.")
         ("mongodb-store-block-states", bpo::value<bool>()->default_value(true),
          "Enables storing block state in mongodb.")
         ("mongodb-store-transactions", bpo::value<bool>()->default_value(true),
          "Enables storing transactions in mongodb.")
         ("mongodb-store-transaction-traces", bpo::value<bool>()->default_value(true),
          "Enables storing transaction traces in mongodb.")
         ("mongodb-store-action-traces", bpo::value<bool>()->default_value(true),
          "Enables storing action traces in mongodb.")
         ("mongodb-expire-after-seconds", bpo::value<uint32_t>()->default_value(0),
          "Enables expiring data in mongodb after a specified number of seconds.")
         ("mongodb-filter-on", bpo::value<vector<string>>()->composing(),
          "Track actions which match receiver:action:actor. Receiver, Action, & Actor may be blank to include all. i.e. eosio:: or :transfer:  Use * or leave unspecified to include all.")
         ("mongodb-filter-out", bpo::value<vector<string>>()->composing(),
          "Do not track actions which match receiver:action:actor. Receiver, Action, & Actor may be blank to exclude all.")
         ;
}

void pgsql_budget_plugin::plugin_initialize(const variables_map& options)
{

}

void pgsql_budget_plugin::plugin_startup()
{
}

void pgsql_budget_plugin::plugin_shutdown()
{
   my->accepted_block_connection.reset();
   my->irreversible_block_connection.reset();
   my->accepted_transaction_connection.reset();
   my->applied_transaction_connection.reset();

   my.reset();
}

} // namespace eosio
