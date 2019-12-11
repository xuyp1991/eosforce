#pragma once

#include <eosio/chain/authority.hpp>
#include <eosio/chain/chain_config.hpp>
#include <eosio/chain/config.hpp>
#include <eosio/chain/types.hpp>
namespace eosio { namespace budget {

using action_name    = eosio::chain::action_name;

const static uint64_t budget_account_name    = N(eosc.budget);

struct approve{
   account_name   approver;
   uint64_t         id;
   std::string    memo;

   static account_name get_account() {
      return budget_account_name;
   }

   static action_name get_name() {
      return N(approve);
   }
};

struct unapprove{
   account_name   approver;
   uint64_t         id;
   std::string    memo;

   static account_name get_account() {
      return budget_account_name;
   }

   static action_name get_name() {
      return N(unapprove);
   }
};

}  }

FC_REFLECT( eosio::budget::approve                           , (approver)(id)(memo) )
FC_REFLECT( eosio::budget::unapprove                           , (approver)(id)(memo) )