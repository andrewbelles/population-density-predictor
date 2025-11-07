/*
 * climate.cpp  Andrew Belles  Nov 7th, 2025 
 *
 * Both an example of Crawler API and data collection of Climate Data 
 * from NOAA Climate Web Service for collection of medium resolution climate data 
 *
 */ 

#include <unordered_map>

#include "crawler/crawler.hpp"
#include "crawler/crawler_support.hpp"

struct Station {}; 
struct Climate {};


class ClimateClient {
public:


private: 
  /********** Custom Typing and Declaration of Handlers ****/ 
  using StationMapType  = std::unordered_map<std::string, std::vector<Station>>;
  using ClimateMapType  = std::unordered_map<std::string, std::vector<Climate>>;
  using StationIterator = typename StationMapType::const_iterator;  
  using ClimateIterator = typename ClimateMapType::const_iterator;  

  crwl::Crawler<Station> station_; 
  crwl::Crawler<Climate> climate_; 

  crwl::Handlers<Station> StationHandler_{ station_handler, station_insert }; 
  crwl::Handlers<Climate> ClimateHandler_{ climate_handler, climate_insert }; 

  static crwl::PageBatch<Station> 
  station_handler(const json::object& obj, const std::vector<crwl::Field>& fields, 
                  const crwl::PaginationState& state)
  {

  }

  static crwl::PageBatch<Climate> 
  climate_handler(const json::object& obj, const std::vector<crwl::Field>& fields, 
                  const crwl::PaginationState& state)
  {

  }

  static int 
  station_insert(StationIterator first, StationIterator last)
  {

  }

  static int 
  climate_insert(ClimateIterator first, ClimateIterator last)
  {

  }

  /********** Private Members *****************************/

};
