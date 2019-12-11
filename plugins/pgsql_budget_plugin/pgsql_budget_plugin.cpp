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

#include <pqxx/pqxx>

#include <eosio/chain_plugin/chain_plugin.hpp>
#include <boost/format.hpp>

#include <eosio/pgsql_budget_plugin/budget_type.hpp>

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
      void wipe_database();

      template<typename Queue, typename Entry> void queue(Queue& queue, const Entry& e);

      bool configured{false};
      bool wipe_database_on_startup{false};
      uint32_t start_block_num = 0;
      std::atomic_bool start_block_reached{true};

      bool is_producer = false;

      bool update_blocks_via_block_num = false;
      bool store_blocks = true;
      bool store_block_states = true;
      bool store_transactions = true;
      bool store_transaction_traces = true;
      bool store_action_traces = true;
      uint32_t expire_after_seconds = 0;

      std::string db_name;

      //std::shared_ptr<fill_postgresql_config>              config;
      std::optional<pqxx::connection>                      sql_connection;


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
         queue( transaction_metadata_queue, t );
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
         queue( irreversible_block_state_queue, bs );
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
         queue( block_state_queue, bs );
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
         _process_accepted_transaction( t );
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
   //ilog("xuyapeng add for test _process_accepted_transaction**********");
}


void pgsql_budget_plugin_impl::_process_applied_transaction( const chain::transaction_trace_ptr& t ) {
   //ilog("xuyapeng add for test _process_applied_transaction//////////");
}

void pgsql_budget_plugin_impl::_process_accepted_block( const chain::block_state_ptr& bs ) {
   //ilog("xuyapeng add for test _process_accepted_block++++++++++");
}

// struct handover_info{
//    std::vector<account_name>members;
//    std::string memo;

//    static account_name get_account() {
//       return N(eosc.budget);
//    }

//    static action_name get_name() {
//       return N(handover);
//    }
// };



void pgsql_budget_plugin_impl::_process_irreversible_block(const chain::block_state_ptr& bs) {
   //ilog("xuyapeng add for test _process_accepted_block-------------");
   auto ro_api = app().get_plugin<chain_plugin>().get_read_only_api();
   pqxx::work t(*sql_connection);
   std::string sqlstr;
   for( const auto& receipt : bs->block->transactions ) {
      string trx_id_str;
      if( receipt.trx.contains<packed_transaction>() ) {
            const auto& pt = receipt.trx.get<packed_transaction>();
            // get id via get_raw_transaction() as packed_transaction.id() mutates internal transaction state
            const auto& raw = pt.get_raw_transaction();
            const auto& trx = fc::raw::unpack<transaction>( raw );

            for (auto &act: trx.actions)
            {
               if (act.name == N(propose) && act.account == N(eosc.budget)) {
                 pqxx::result res = t.exec("select max(motion_id) from b_motions where motion_type = 0;");

                 std::string Lower = res[0]["max"].c_str();

                  eosio::chain_apis::read_only::get_table_rows_params motions_get{true,N(eosc.budget),"eosc.budget",N(motions),"",Lower,"",2,"","","dec",true,false};
                  auto result = ro_api.get_table_rows(motions_get);
                  auto motion_info = result.rows[result.rows.size() - 1];

                  auto proposer_str = motion_info["proposer"].as_string();

                  std::string insert_sql = str(boost::format("insert into b_motions(motion_id,root_id,title,content,quantity,proposer,section,takecoin_num,approve_end_block_num,extern_data,motion_type) "
                  "values (%s,%s,\'%s\',\'%s\',\'%s\',\'%s\',%s,%s,%s,\'%s\',%d);") % motion_info["id"].as_string()
                     % motion_info["root_id"].as_string()
                     % motion_info["title"].as_string()
                     % motion_info["content"].as_string()
                     % motion_info["quantity"].as_string()
                     % proposer_str 
                     % motion_info["section"].as_string() 
                     % motion_info["takecoin_num"].as_string() 
                     % motion_info["approve_end_block_num"].as_string()
                     % ""
                     % 0);

                  t.exec(insert_sql);

                  auto approve_res = t.exec("select max(id) from b_approves where proposer = \'" + proposer_str + "\';");
                  Lower = approve_res[0]["max"].c_str();
                  eosio::chain_apis::read_only::get_table_rows_params approves_get{true,N(eosc.budget),proposer_str,N(approvers),"",Lower,"",2,"","","dec",true,false};
                  auto result_approve = ro_api.get_table_rows(motions_get);
                  auto approve_info = result.rows[result.rows.size() - 1]["id"].as_string();

                  std::string insert_approve = "insert into b_approves(proposer,id,requested) select \'"+ motion_info["proposer"].as_string()+ "\',"+ motion_info["id"].as_string() +",member from b_members order by member_serial desc limit 1;";
                  ilog("xuyapeng add for test motions -----------------");
                  t.exec(insert_approve);
               }
               else if ( act.name == N(handover) && act.account == N(eosc.budget) ) {
                  eosio::chain_apis::read_only::get_table_rows_params members_get{true,N(eosc.budget),"eosc.budget",N(committee),"","","",1,"","","dec",true,false};
                  auto result = ro_api.get_table_rows(members_get);
                  auto member_info = result.rows[result.rows.size() - 1]["member"];
                  
                  auto member_array = member_info.get_array();
                  auto isize = member_array.size();
                  std::string member_str = "";
                  for ( int i=0; i!= isize; ++i ) {
                     auto test = member_array[i].as_string();                     
                     member_str += "\"" + test + "\",";
                  }
                  member_str = member_str.substr(0, member_str.length() - 1);
                  std::string insert_sql = "insert into b_members(member) values(\'{" + member_str + "}\');";
                  ilog("xuyapeng add for test handover -----------------");
                  t.exec(insert_sql);
               }
               else if ( act.name == N(approve) && act.account == N(eosc.budget) ) {
                  auto approve_info = act.data_as<budget::approve>();
                  auto approver = approve_info.approver.to_string();
                  auto id_str = fc::to_string(approve_info.id);
                  std::string update_approve_str = "update b_approves set approved = approved || \'{\"" + approver +
                        "\"}\',requested = array_remove(requested,\'" + approver +"\') where id = "+ id_str +";";
                  t.exec(update_approve_str);
                  std::string get_num = "select array_length(approved,1) as lenapp , array_length(requested,1) as lenreq , array_length(unapproved,1) as lenunapp from b_approves where id = "+ id_str +";";
                  ilog("---xuyapeng add test--- ${d}",("d",get_num));
                  auto num_res = t.exec(get_num);
                  auto len_app = num_res[0]["lenapp"].as<std::optional <int >>();
                  auto len_req = num_res[0]["lenreq"].as<std::optional <int >>();
                  auto len_unapp = num_res[0]["lenunapp"].as<std::optional <int >>();
                  int app_num = 0,total_req = 0;
                  if (len_app.has_value()) {
                     app_num = len_app.value();
                     total_req += app_num;
                  }
                  if (len_req.has_value()) {
                     total_req += len_req.value();
                  }
                  if (len_unapp.has_value()) {
                     total_req += len_unapp.value();
                  }
                  if ( app_num > total_req * 2 / 3 ) {
                     //update motions
                     std::string update_motion = "update b_motions set section = 1 where motion_id = "+ id_str +";";
                     t.exec(update_motion);
                  }
               }
               else if ( act.name == N(unapprove) && act.account == N(eosc.budget) ) {
                  auto approve_info = act.data_as<budget::unapprove>();
                  auto approver = approve_info.approver.to_string();
                  auto id_str = fc::to_string(approve_info.id);
                  std::string update_approve_str = "update b_approves set unapproved = unapproved || \'{\"" + approver +
                        "\"}\',requested = array_remove(requested,\'" + approver +"\') where id = "+ id_str +";";
                  t.exec(update_approve_str);
                  std::string get_num = "select array_length(approved,1) as lenapp , array_length(requested,1) as lenreq , array_length(unapproved,1) as lenunapp from b_approves where id = "+ id_str +";";
                  ilog("---xuyapeng add test--- ${d}",("d",get_num));
                  auto num_res = t.exec(get_num);
                  auto len_app = num_res[0]["lenapp"].as<std::optional <int >>();
                  auto len_req = num_res[0]["lenreq"].as<std::optional <int >>();
                  auto len_unapp = num_res[0]["lenunapp"].as<std::optional <int >>();
                  int unapp_num = 0,total_req = 0;
                  if (len_app.has_value()) {
                     total_req += len_app.value();
                  }
                  if (len_req.has_value()) {
                     total_req += len_req.value();
                  }
                  if (len_unapp.has_value()) {
                     unapp_num = len_unapp.value();
                     total_req += unapp_num;
                  }
                  if ( unapp_num > total_req  / 3 ) {
                     //update motions
                     std::string update_motion = "update b_motions set section = 2 where motion_id = "+ id_str +";";
                     t.exec(update_motion);
                  }
               }
            }
      }
   }
   t.commit();
}


pgsql_budget_plugin_impl::pgsql_budget_plugin_impl() {
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

void pgsql_budget_plugin_impl::wipe_database() {
   ilog("pgsql db wipe_database");

   pqxx::work t(*sql_connection);

   t.exec("DROP TABLE IF EXISTS test_tbl;");
   t.commit();
   ilog("done wipe_database");
}


void pgsql_budget_plugin_impl::init() {
   //这个地方可以使用      
   // std::string table_name = "testtable";
   // pqxx::work t(*sql_connection);

   // t.exec("CREATE TABLE test_tbl(name VARCHAR(20), signup_date DATE);"
   //       );

   // t.commit();

   // ilog("xuyapeng add test for create table");

   consume_thread = std::thread( [this] {
      fc::set_os_thread_name( "pgsql_plugin" );
      ilog("xuyapeng add for test init");
      consume_blocks();
   } );
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
      ("pgsql-wipe", bpo::bool_switch()->default_value(false),
      "Required with --replay-blockchain, --hard-replay-blockchain, or --delete-all-blocks to wipe pgsql db."
      "This option required to prevent accidental wipe of pgsql db.")
      ("pgsql-block-start", bpo::value<uint32_t>()->default_value(0),
      "If specified then only abi data pushed to mongodb until specified block is reached.")
      ("pgsql-uri,m", bpo::value<std::string>(),
      "pgsql URI connection string"
            " If not specified then plugin is disabled."
            " Example: dbname=xuyptest hostaddr=127.0.0.1 user=xuyp password=123456 ")
      ;
}

void pgsql_budget_plugin::plugin_initialize(const variables_map& options)
{
   ilog("xuyapeng add for test pgsql_budget_plugin::plugin_initialize");
   if( options.count( "pgsql-uri" )) {
      if( options.count( "mongodb-block-start" )) {
         my->start_block_num = options.at( "mongodb-block-start" ).as<uint32_t>();
      }

      if( options.at( "pgsql-wipe" ).as<bool>()) {
         ilog( "Wiping pgsql database on startup" );
         my->wipe_database_on_startup = true;
      }
      ilog( "Wiping pgsql database on 459" );
      std::string uri_str = options.at( "pgsql-uri" ).as<std::string>();
      my->sql_connection = pqxx::connection(uri_str);  
            ilog( "Wiping pgsql database on 462" );

      if( my->wipe_database_on_startup ) {
         my->wipe_database();
      }

      chain_plugin* chain_plug = app().find_plugin<chain_plugin>();
      EOS_ASSERT( chain_plug, chain::missing_chain_plugin_exception, ""  );
      auto& chain = chain_plug->chain();
      my->chain_id.emplace( chain.get_chain_id());

      my->accepted_block_connection.emplace( chain.accepted_block.connect( [&]( const chain::block_state_ptr& bs ) {
         my->accepted_block( bs );
      } ));
      my->irreversible_block_connection.emplace(
            chain.irreversible_block.connect( [&]( const chain::block_state_ptr& bs ) {
               my->applied_irreversible_block( bs );
            } ));
      my->accepted_transaction_connection.emplace(
            chain.accepted_transaction.connect( [&]( const chain::transaction_metadata_ptr& t ) {
               my->accepted_transaction( t );
            } ));
      my->applied_transaction_connection.emplace(
            chain.applied_transaction.connect( [&]( std::tuple<const chain::transaction_trace_ptr&, const chain::signed_transaction&> t ) {
               my->applied_transaction( std::get<0>(t) );
            } ));

      my->init();
   }
   
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
