/*
 * crawler.hpp  Andrew Belles  Nov 6th, 2025 
 *
 * Base Crawler. Implements request functionality and allows for custom handlers  
 * Which provide a layer of abstraction for both parsing requested json from some 
 * api and also handling insertion into an sqlite database 
 */ 

#pragma once 

#include <algorithm>
#include <chrono>
#include <exception>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread> 
#include <unordered_map>
#include <vector> 

#include <boost/json.hpp>  
#include "crawler_support.hpp"
#include "http.hpp"
#include "json.hpp"

namespace crwl {

namespace json = boost::json;

/************ crwl::Crawler *******************************/
/* Crawler Interface
 *
 * Templates: 
 *   Item: A struct that contains a single JSON block from whatever API hit. 
 *
 * TODO: 
 *   - Finish Pagination 
 *   - Introduce trait bounds on Item such that we guarantee the json and sqlite 
 *     handlers have deterministic behavior when acting on Item 
 *
 */
template <class Item> 
class Crawler {
public:

  Crawler(
    const CrawlerMetadata& m,
    const Handlers<Item>& h, 
    size_t retries = 5
  ) : meta_(m), fns_(h) {} 

  /********** crwl::cycle() *******************************/ 
  /* Fetches data over all endpoints into map 
   * Sends range of map into sqlite_handler to parse into database 
   */
  void cycle() 
  { 
    item_map_.clear(); 
    try {
      // For each endpoint fetch 
      for (auto& endpoint : meta_.endpoints) {
        drain_endpoint_(endpoint);
      }
    } catch (const std::exception& e) {
      throw std::runtime_error(std::string("crwl::cycle: fetch choked: ") + e.what());
    }
    
    // Call over range of elements in map from previous fetch 
    fns_.sqlite_handler(item_map_.cbegin(), item_map_.cend());
  }

private:
  std::unordered_map<std::string, std::vector<Item>> item_map_{};
  CrawlerMetadata meta_;
  Handlers<Item> fns_; 
  size_t retries{5}; 

  /********** drain_endpoint_() ***************************/
  /* For each endpoint, make request and drain the endpoint until PaginationState 
   * reaches end state. 
   *
   * Crawler Provides: 
   *   the individual endpoint that should be requested at 
   *
   * Crawler modifies: 
   *   Item_map has items in batch insterted per field 
   */
  void 
  drain_endpoint_(const Endpoint& endpoint)
  {
    PaginationState state{}; 
    do {
      auto batch = fetch_(endpoint, state);
      append_batch_(endpoint.fields, batch); 
      update_state_(endpoint.pagination, batch, state);

      if ( !state.exhausted && endpoint.pagination.page_delay.count() > 0 ) {
        std::this_thread::sleep_for(endpoint.pagination.page_delay);
      }
    } while ( !state.exhausted ); 
  }

  /********** fetch() *************************************/
  /* Executes a single request on the specified endpoint, handles Pagination through
   * a FSM stored in PaginationState struct 
   * 
   * Crawler Provides: 
   *   Endpoint of API to hit on 
   *   Current state of Paging FSM 
   *
   * Returns to Crawler: 
   *   Returns a PageBatch struct to be handled within drain_endpoint_
   */ 
  PageBatch<Item>
  fetch_(const Endpoint& endpoint, const PaginationState& state) 
  {
    const std::string url = build_url_(meta_.base_url, endpoint, state); 
    try {
      // get body of request and parse into value  
      std::string body = request_with_retries_(url);
      const json::object& root = jsc::as_obj(jsc::parse(body)); 

      auto batch = fns_.json_handler(root, endpoint.fields, state); 
      if ( endpoint.fields.size() != batch.items.size() ) {
        throw std::runtime_error("resulting item vector is too small");
      }
      return batch; 
    } catch (...) {
      std::throw_with_nested(std::runtime_error("crwl::fetch failed: " + url)); 
    }
  }

  /********** crwl::request_with_retries_() ***************/ 
  /* Makes requests at the built url for specified number of retries 
   * 
   * Caller Provides: 
   *   String representing the url to be made and requested at 
   *
   * We return: 
   *   The json returned as a string (or "" and exception for error)
   */
  std::string 
  request_with_retries_(const std::string& url) const 
  {
    std::string current = url; 
    auto backoff = std::chrono::milliseconds(200);

    // Limit to some maximum number of retries
    for (size_t attempt{1}; attempt <= retries; attempt++) {
      // Exception is for is_retryable or critical error 
      try {
        auto l_url = htc::parse_url(current); 
        auto [res, status] = htc::request(meta_.api_key, l_url);
        
        // implies a redirect 
        if ( status == 1 ) {
          current = res; 
          attempt -= 1; 
          continue; 
        } else if ( status == 0 ) {
          return std::move(res); 
        }
      } catch (const std::exception& e) {
        if ( attempt == retries ) {
          throw std::runtime_error(
              std::string("failure to complete request: ") + e.what());
        }

        std::this_thread::sleep_for(backoff);
        backoff *= 2; 
        continue; 
      }
    }

    throw std::runtime_error("request_with_retries_: exhausted attemps w/o resp");
  }

  /********** append_batch_() *****************************/ 
  /* Appends an entire batch onto the item_map using the fields associated with
   * the current endpoint. 
   * 
   * Does so safely making the assumption that fields.size() != batch.size() 
   * necessarily 
   *
   * Caller Provides: 
   *   Field vector and current batch from page 
   *
   * We modify: 
   *   Item_map has elements inserted for each field that can be associated with 
   *   an element from the PageBatch 
   */  
  void 
  append_batch_(const std::vector<Field>& fields, const PageBatch<Item>& batch)
  {
    if ( fields.empty() || batch.items.empty() ) {
      return;
    }

    const size_t columns = std::min(fields.size(), batch.items.size());
    for (size_t i{0}; i < columns; i++) {
      const auto& field = fields[i];
      const auto& chunk = batch.items[i];
      auto& bucket = item_map_[field.alias.value_or(field.name)];
      bucket.insert(bucket.end(), chunk.begin(), chunk.end());
    }
  }

  /********** update_state_() *****************************/ 
  /* Updates the state of the PaginationState FSM depending on the previous 
   * batch. Uses metadata stored within the PageBatch struct to make determinations 
   * on how FSM should be updated 
   *
   * Caller Provides: 
   *   PaginationConfig 
   *   Last PageBatch 
   *   Current PaginationState
   *
   * We modify: 
   *   PaginationState to update into next state 
   */ 
  void 
  update_state_(const PaginationConfig& cfg, const PageBatch<Item>& batch,
                PaginationState& state) 
  {
    size_t rows = 0;
    for (const auto& column : batch.items) {
      rows = std::max(rows, column.size());
    }

    // update counters, move cursor
    state.items_seen += rows;
    state.page_index += 1;
    state.cursor = batch.cursor; 

    // check all possible flags
    const bool reached_max = cfg.max_pages > 0 && state.page_index >= cfg.max_pages;
    const bool pagination_disabled = cfg.mode == PaginationConfig::Mode::None;
    const bool cursor_missing = (cfg.mode == PaginationConfig::Mode::Cursor) &&
                                batch.has_more && !batch.cursor.has_value();
    const bool no_more = pagination_disabled ? true : !batch.has_more;

    // move to end state if any flag is raised 
    state.exhausted = reached_max || cursor_missing || no_more;
  }
}; 

} // end namespace crwl 
