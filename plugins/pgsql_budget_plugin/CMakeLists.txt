
  find_package(PostgreSQL COMPONENTS Libraries)


  # This needs to be after the else/return in the situation that libmongoc isn't found and we need to avoid building mongo :: 'bsoncxx/builder/basic/kvp.hpp' file not found
  file(GLOB HEADERS "include/eosio/pgsql_budget_plugin/*.hpp")
  add_library( pgsql_budget_plugin
               pgsql_budget_plugin.cpp
               ${HEADERS} )

  target_include_directories(pgsql_budget_plugin
          PRIVATE ${LIBMONGOCXX_STATIC_INCLUDE_DIRS} ${LIBBSONCXX_STATIC_INCLUDE_DIRS}
          PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include"
          )

  target_compile_definitions(pgsql_budget_plugin
    PRIVATE ${LIBMONGOCXX_STATIC_DEFINITIONS} ${LIBBSONCXX_STATIC_DEFINITIONS}
    )

  if (PostgreSQL_INCLUDE_DIR)
    set(SKIP_PQXX_SHARED ON)
    set(SKIP_BUILD_TEST ON)
    set(PQXX_LIBRARIES pqxx_static)
  else()
    set(PQXX_LIBRARIES "")
  endif()

  target_link_libraries(pgsql_budget_plugin
          PUBLIC chain_plugin eosio_chain appbase
          ${LIBMONGOCXX_STATIC_LIBRARY_PATH} ${LIBBSONCXX_STATIC_LIBRARY_PATH}
          ${MONGOC_STATIC_LIBRARY} ${BSON_STATIC_LIBRARY}
          resolv ${LINK_SASL} ${LINK_ICU} ${LINK_SNAPPY}
          )

