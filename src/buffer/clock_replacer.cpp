#include <cstdio>
#include <thread>  // NOLINT
#include <vector>
#include "buffer/clock_replacer.h"
namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
   using std::vector; 
   for(size_t i = 0; i != num_pages; i++){
    	replacer.emplace_back(-1);
   }
   framesInBuffer = num_pages;
   clockHand = 0;
   framesInReplacer = 0;
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
 latch.lock();
 
  if (framesInReplacer <= 0) {
    latch.unlock();
    return false;
  }
  
  while (true) {
    if ( replacer[clockHand] == -1) {
      clockHand = (clockHand + 1) % framesInBuffer;
      continue;
    }
    
    if (replacer[clockHand] == 1) {
      replacer[clockHand] = 0;
      clockHand = (clockHand + 1) % framesInBuffer;
      continue;
    }
    	
    *frame_id = clockHand + 1;
     replacer[clockHand] = -1;
     framesInReplacer--;
    break;
  }  
  latch.unlock();
  return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  latch.lock();
  if (replacer[frame_id - 1] !=- 1) {
    replacer[frame_id - 1] = -1;
    framesInReplacer--;
  }
  latch.unlock();
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  latch.lock();
  if (replacer[frame_id - 1] == -1) {
  	replacer[frame_id - 1] = 1;
  	framesInReplacer++;
  }
  latch.unlock();
}
size_t ClockReplacer::Size() { return framesInReplacer; }
}  // namespace bustub
