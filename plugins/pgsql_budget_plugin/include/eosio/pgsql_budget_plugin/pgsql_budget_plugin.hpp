/**
 *  @file
 *  @copyright defined in eos/LICENSE
 */
#pragma once

#include <eosio/chain_plugin/chain_plugin.hpp>
#include <appbase/application.hpp>
#include <memory>

namespace eosio {

using pgsql_budget_plugin_impl_ptr = std::shared_ptr<class pgsql_budget_plugin_impl>;

/**
 * Provides persistence to MongoDB for:
 * accounts
 * actions
 * block_states
 * blocks
 * transaction_traces
 * transactions
 * pub_keys
 * account_controls
 *
 *   See data dictionary (DB Schema Definition - EOS API) for description of MongoDB schema.
 *
 *   If cmake -DBUILD_MONGO_DB_PLUGIN=true  not specified then this plugin not compiled/included.
 */
class pgsql_budget_plugin : public plugin<pgsql_budget_plugin> {
public:
   APPBASE_PLUGIN_REQUIRES((chain_plugin))

   pgsql_budget_plugin();
   virtual ~pgsql_budget_plugin();

   virtual void set_program_options(options_description& cli, options_description& cfg) override;

   void plugin_initialize(const variables_map& options);
   void plugin_startup();
   void plugin_shutdown();

private:
   pgsql_budget_plugin_impl_ptr my;
};

}

