#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"


    namespace bustub {

  /**
   * ClockReplacer implements the clock replacement policy, which approximates the Least Recently Used policy.
   */
  class ClockReplacer : public Replacer {
   public:
    /**
     * Create a new ClockReplacer.
     * @param num_pages the maximum number of pages the ClockReplacer will be required to store
     */
    explicit ClockReplacer(size_t num_pages);

    /**
     * Destroys the ClockReplacer.
     */
    ~ClockReplacer() override;

    bool Victim(frame_id_t *frame_id) override;

    void Pin(frame_id_t frame_id) override;

    void Unpin(frame_id_t frame_id) override;

    size_t Size() override;

   private:
   
	size_t clockHand;
    size_t framesInReplacer;
    size_t framesInBuffer;
    std::vector<int> replacer;
    std::mutex latch;
  };

}  // namespace bustub
