/* Copyright 2026 - 2026 wzycc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */
// #include "collector/filemon_bpf_collector.hpp"

// /* -------------------------------------- */
// bool FileMonCollector::load_and_attach(){
//     try { loader_ = std::make_unique<BpfLoader>("filemon.bpf.o"); }
//     catch (...) { return false; }
//     rb_map_fd_  = bpf_object__find_map_fd_by_name(loader_->obj(), "rb");
//     pid_map_fd_ = bpf_object__find_map_fd_by_name(loader_->obj(), "pid_target");
//     if (rb_map_fd_ < 0 || pid_map_fd_ < 0) return false;

//     const char* names[] = {"vfs_read", "vfs_write", "do_sys_openat2", "filp_close"};
//     for (auto n : names) {
//         bpf_program* p = bpf_object__find_program_by_name(loader_->obj(), n);
//         if (!p || !bpf_program__attach(p)) return false;
//     }
//     rb_ = ring_buffer__new(rb_map_fd_, event_cb, &tl_events, nullptr);
//     return rb_ != nullptr;
// }

// void FileMonCollector::stop() noexcept{
//     if (rb_) { ring_buffer__free(rb_); rb_ = nullptr; }
//     loader_.reset();
//     attached_ = false;
// }

// bool is_permutation(const std::vector<int>& a,
//                     const std::vector<int>& b) {
//     if (a.size() != b.size()) return false;
//     std::vector<int> ca = a;
//     std::vector<int> cb = b;
//     std::sort(ca.begin(), ca.end());
//     std::sort(cb.begin(), cb.end());
//     return ca == cb;
// }

// void FileMonCollector::update_pid_filter(const int jobid, const std::vector<int>& pids){
//     auto& cache = pids_filter_cache[jobid];
//     std::vector<int> tmp = pids;
//     std::sort(tmp.begin(), tmp.end());
//     if (tmp != cache) {
//         cache = std::move(tmp);               
//         u8 dummy = 1;
//         for (u32 pid : pids){
//             bpf_map_update_elem(pid_map_fd_, &pid, &dummy, BPF_ANY);
//         }
//     }
// }


// CollectResult FileMonCollector::collect(const Job& job){
//     if (!attached_) {
//         if (!load_and_attach() || !attached_.exchange(true)){
//             throw std::runtime_error("attach error");
//         }
//     }
//     update_pid_filter(job.JobID, job.JobPIDs);

//     std::vector<event> job_events;
//     auto cnt = ring_buffer__consume(rb_);
//     auto new_end = std::remove_if(tl_events.begin(), tl_events.end(),
//                                   [&job, &job_events](const event& ev) {
//                                     for(const int& job_pids: job.JobPIDs){
//                                         if(ev.pid == job_pids){
//                                             job_events.push_back(ev);
//                                             return true;
//                                         }
//                                     }
//                                     return false;
//                                   });
//     tl_events.erase(new_end,tl_events.end());

//     return job_events;
// }

