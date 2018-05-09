#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <iostream>
#include <vector>
#include <string>
#include <vector>
#include <unordered_map>
#include "omp.h"
#include <ctime>

#define CHUNKSIZE 10
#define N 10

using namespace std;

using tab_t = vector<vector<string>>;
tab_t tab1 {
// Age  Name
          {"27", "Jonah"}
        , {"18", "Alan"}
        , {"28", "Glory"}
        , {"18", "Popeye"}
        , {"28", "Alan"}
};

tab_t tab2 {
// Character  Nemesis
          {"Jonah", "Whales"}
        , {"Jonah", "Spiders"}
        , {"Alan", "Ghosts"}
        , {"Alan", "Zombies"}
        , {"Glory", "Buffy"}
};
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
    unordered_multimap<string, size_t> hashmap;
    // hash
    #pragma omp for
    for(size_t i = 0; i < a.size(); ++i) {
        hashmap.insert(make_pair(a[i][columna], i));
    }
    // map
    tab_t result;
    for(size_t i = 0; i < b.size(); ++i) 
    {
        auto range = hashmap.equal_range(b[i][columnb]);    
        #pragma omp parallel 
        {
            #pragma omp single 
            {
                for(auto it = range.first; it != range.second; ++it) 
                {
                    #pragma omp task 
                    {
                        tab_t::value_type row;
                        row.insert(row.end(), a[it->second].begin(), a[it->second].end());
                        row.insert(row.end(), b[i].begin(), b[i].end());
                        result.push_back(move(row));
                    }
                }
            }
        }
    }
    return result;
}

// using namespace std;
// int main() {

//     int nthreads, tid, i;
//     float a[N], b[N], c[N];


//     /* Some initializations */
//     for (i=0; i < N; i++)
//     {
//         a[i] = b[i] = i;
//         cout << a[i] << endl;
//     }
//     /* Fork a team of threads giving them their own copies of variables */
//     #pragma omp parallel private(nthreads, tid)
//     {
//         /* Obtain thread number */
//         tid = omp_get_thread_num();
//         printf("Hello World from thread = %d\n", tid);

//         /* Only master thread does this */
//         if (tid == 0) 
//         {
//             nthreads = omp_get_num_threads();
//             printf("Number of threads = %d\n", nthreads);
//         }
//     } /* All threads join master thread and disband */

//     #pragma omp for 
//     for (i = 0; i < N; i++)
//     {
//         c[i] = a[i] + b[i];
//         printf("Thread %d: c[%d]= %f\n",tid,i,c[i]);
//     }

//     // #pragma omp for 
//     // for (auto i : a)
//     // {
//     //     cout << a << endl;
//     // }

//         vector<int> v = {0, 1, 2, 3, 4, 5};
//     // #pragma omp for 
//     for (const int i : v) { // access by const reference
//         cout << i << ' ';
//     }

// }

int main()
{   
    clock_t begin = clock();
    cout << "Table A: "       << endl << tab1 << endl;
    cout << "Table B: "       << endl << tab2 << endl;
    auto tab3 = Join(tab1, 1, tab2, 0);
    cout << "Joined tables: " << endl << tab3 << endl;
    clock_t end = clock();
    double elapsed_secs = double(end - begin) / CLOCKS_PER_SEC;
    cout << "Time Bitches: " << elapsed_secs << endl;
}
