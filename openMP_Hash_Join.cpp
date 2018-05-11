#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <iostream>
#include <vector>
#include <string>
#include <string.h>
#include <unordered_map>
#include <ctime>
#include "omp.h"
#include <typeinfo>
#include <iterator>

using namespace std;
using tab_t = vector<vector<string>>;

ostream& operator<<(ostream& o, const tab_t& t) {
    for(size_t i = 0; i < t.size(); ++i) {
        o << i << ":";
        for(const auto& e : t[i])
            o << '\t' << e;
        o << endl;
    }
    return o;
}

tab_t Join(const tab_t& a, size_t columna, const tab_t& b, size_t columnb) {
    std::unordered_multimap<std::string, size_t> hashmap;
    // hash
    clock_t begin = clock(); 
        for(size_t i = 0; i < a.size(); ++i) {
            hashmap.insert(std::make_pair(a[i][columna], i));
        }
    clock_t end = clock();
    double elapsed_secs = double(end - begin) / CLOCKS_PER_SEC;
    cout << "Time for hash: " << elapsed_secs << endl;

    clock_t begin1 = clock();
    // map
    tab_t result;
    size_t i;
 
    for(i = 0; i < b.size(); ++i) {
        auto range = hashmap.equal_range(b[i][columnb]);
        //int count = std::distance(range.first, range.second);
            for (int j = 0; j != std::distance(range.first, range.second); ++j)
            {
                auto it = range.first;
                std::advance(it, j);
                tab_t::value_type row;
                row.insert(row.end(), a[it->second].begin(), a[it->second].end());
                row.insert(row.end(), b[i].begin(), b[i].end());
                result.push_back(move(row));
            }
            // for(auto it = range.first; it != range.second; ++it) 
            // {
            //         tab_t::value_type row;
            //         row.insert(row.end(), a[it->second].begin(), a[it->second].end());
            //         row.insert(row.end(), b[i].begin(), b[i].end());
            //         result.push_back(move(row));
            // }
    }
    clock_t end1 = clock();
    double elapsed_secs1 = double(end1 - begin1) / CLOCKS_PER_SEC;
    cout << "Time for map: " << elapsed_secs1 << endl;
    return result;
}

int main() {
    int numRows = 7000;
    vector<string> tempRow;
    string toInsert;
    tab_t table1;
    for (int i = 0; i != numRows; i++)
    {
        toInsert = to_string(i);
        toInsert = "Join" + toInsert;
        tempRow.push_back(toInsert);
        tempRow.push_back(to_string(i));
        table1.push_back(tempRow);
        tempRow.clear();
    }

    tab_t table2;
    for (int i = 0; i != numRows; i++)
    {
        toInsert = to_string(i);
        toInsert = "Join" + toInsert;
        tempRow.push_back(toInsert);
        tempRow.push_back(to_string(i));
        table2.push_back(tempRow);
        tempRow.clear();
    }

    clock_t begin = clock(); 
    auto tab3 = Join(table1, 0, table2, 0);
    clock_t end = clock();
    //cout << "Joined tables: " << endl << tab3 << endl;
    cout << numRows << endl;
    double elapsed_secs = double(end - begin) / CLOCKS_PER_SEC;
    cout << "Time Total: " << elapsed_secs << endl;

}