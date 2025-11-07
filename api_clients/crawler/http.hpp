/*
 * http.hpp  Andrew Belles  Nov 7th, 2025 
 * 
 * Provides Interface for exposed http helper functions for Crawler 
 *
 *
 */ 

#pragma once 

#include <string_view> 

namespace htc {

struct Url {}; 

/************ parse_url() *********************************/ 
/*
 *
 */ 
Url parse_url(std::string_view url);

/************ request() ***********************************/ 
/*
 *
 */ 
std::pair<std::string, int> request(const std::string& key, const Url& url);

}
