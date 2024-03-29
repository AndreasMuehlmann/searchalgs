# searchalgs

length array searched in 10^6, length array searched for 10^5  
    - linear multiple search: 5164 us
    - split search: 4148 us
    - parallel linear multiple search: 1299 us
    - parallel split search: 969 us


for length array searched in 10^5, length array searched for 10^4 
normal split search is still faster than the parallel version

split search is better when there is a large amount of elements
searched for compared to the ones searched in
