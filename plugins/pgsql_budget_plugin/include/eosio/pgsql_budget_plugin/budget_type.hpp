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

struct takecoin{
   account_name   proposer;
   uint64_t       montion_id;
   std::string    content;
   asset          quantity;

   static account_name get_account() {
      return budget_account_name;
   }

   static action_name get_name() {
      return N(takecoin);
   }
};

struct agreecoin{
   account_name   approver;
   account_name   proposer;
   uint64_t       id;
   std::string    memo;

   static account_name get_account() {
      return budget_account_name;
   }

   static action_name get_name() {
      return N(agreecoin);
   }
};

struct unagreecoin{
   account_name   approver;
   account_name   proposer;
   uint64_t       id;
   std::string    memo;

   static account_name get_account() {
      return budget_account_name;
   }

   static action_name get_name() {
      return N(unagreecoin);
   }
};

}  }

FC_REFLECT( eosio::budget::approve                           , (approver)(id)(memo) )
FC_REFLECT( eosio::budget::unapprove                           , (approver)(id)(memo) )
FC_REFLECT( eosio::budget::takecoin                           , (proposer)(montion_id)(content)(quantity) )
FC_REFLECT( eosio::budget::agreecoin                           , (approver)(proposer)(id)(memo) )
FC_REFLECT( eosio::budget::unagreecoin                           , (approver)(proposer)(id)(memo) )