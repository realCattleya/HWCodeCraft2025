#include "./include/controller.h"
#include <iostream> 
#include <cassert>
#include <cmath>
#include <algorithm>
#include <random>
using namespace std;

StorageController::StorageController(int T, int M, int N, int V, int G,
    vector<vector<int>>& del,
    vector<vector<int>>& write,
    vector<vector<int>>& read)
: T(T), M(M), N(N), V(V), G(G),
fre_del(del), fre_write(write), fre_read(read) {
    for(int i=1; i<=N; ++i){
        disks.push_back(new Disk(i, V));
    }

    int time_intervals = (T + FRE_PER_SLICING - 1) / FRE_PER_SLICING;
    tag_hotness.assign(M + 1, vector<double>(time_intervals, 0.0));
    for (int i = 1; i < 4; ++i) {
        std::vector<std::vector<int>> layer;
        for (int x = 1; x < N; ++x) {
            std::vector<int> row;
            for (int y = 0; y < N; ++y) {
                row.push_back(((i * x + y) % N)+1); // 有限域构造法
            }
            layer.push_back(row);
        }
        latin_templates.push_back(layer);
    }
}

void StorageController::pre_process(){
    int time_intervals = (T + FRE_PER_SLICING - 1) / FRE_PER_SLICING;

    // 逐个时间段计算每个 tag 的热度
    vector<long long> tags_size_sum;
    for (int tag = 1; tag <= M; ++tag) {
        tags_size_sum.push_back(fre_write[tag - 1][0]);
        for (int t = 0; t < time_intervals; ++t) {
            long long total_write = fre_write[tag - 1][t];
            long long total_read = fre_read[tag - 1][t];
            // 计算热度：读取量 / (写入量 + 1)，防止除零
            tag_hotness[tag][t] = (double)total_read / (total_write + 1);
        }
    }
    for (Disk* disk : disks){
        // 以第一个片段的写入量为划分比
        disk->partition_units(tags_size_sum);
    }
    cout << "OK" << endl;
    cout.flush();
}

void StorageController::timestamp_align(){
    string cmd;
    cin >> cmd >> current_time;
    assert(cmd == "TIMESTAMP");

    // 计算当前时间片对应的时间段索引
    current_time_interval = current_time / FRE_PER_SLICING;

    // 删除所有过期请求
    auto it = requests.begin();
    while (it != requests.end())
    {
        auto req = it->second;
        if (req->expire_time <= current_time) {
            if (req->target->active_requests.find(req->req_id) != req->target->active_requests.end()) {
                req->target->active_requests.erase(req->req_id);
                req->target->invalid_requests.push_back(req->req_id);
            }
            
            it = requests.erase(it);
        } else {
            it++;
        }
    }
    

    cout << "TIMESTAMP " << current_time << endl;
    cout.flush();
}

vector<int> StorageController::handle_delete(const vector<int>& obj_ids){
    vector<int> aborted;
    for(int obj_id : obj_ids) {
        auto it = objects.find(obj_id);
        if(it == objects.end()) continue;
        // 清除该对象在各盘中占用的单元
        for(auto& rep : it->second->replicas) {
            Disk* disk = disks[rep.disk_id-1];
            for (int u : rep.units) {
                disk->erase(u);
            }
        }
        // 取消对应的读取请求
        for(int req_id : it->second->active_requests) {
            aborted.push_back(req_id);
            requests.erase(req_id);
        }
        for(int req_id : it->second->invalid_requests) {
            aborted.push_back(req_id);
            requests.erase(req_id);
        }
        objects.erase(it);
    }
    return aborted;
}

void StorageController::delete_action(){
    // recieve input
    int n_del;
    cin >> n_del;
    vector<int> del_objs(n_del);
    for(int i=0; i<n_del; ++i)  cin >> del_objs[i];
    // handle_delete
    auto aborted = handle_delete(del_objs);
    cout << aborted.size() << endl;
    for(int id : aborted)   cout << id << endl;
    cout.flush();
}



bool StorageController::write_object(int id, int size, int tag) {
    // 如果使用  id%(N*(N-1)) 的方式来选择正交拉丁表的元组，会出现disk号小先塞满的情况。
    // 这可能是由于没有考虑到size大小的问题
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, N*(N-1)); 
    int random_number = dis(gen);
    int x = id%(N*(N-1))/N;
    int y = id%(N*(N-1))%N;
    //int x = random_number%(N*(N-1))/N;
    //int y = random_number%(N*(N-1))%N;
    int latin_assign_disk_ids[3] = {latin_templates[0][x][y],latin_templates[1][x][y],latin_templates[2][x][y]};
    vector<Disk*> candidates;
    candidates.reserve(disks.size());
    bool is_latin_layout = true;
    for (auto d : disks) {
        if (d->id == latin_templates[0][x][y] || 
            d->id == latin_templates[1][x][y] || 
            d->id == latin_templates[2][x][y]){
            if (d->used_capcity + size <= V){
                candidates.push_back(d);
            }
        }
    }
    if (candidates.size() < REP_NUM) 
        is_latin_layout = false;
    if(!is_latin_layout){
        candidates.clear();
        for (auto &d : disks) {
            if (d->used_capcity + size <= V)
                candidates.push_back(d);
        }
        // 按当前已用单元数量排序，选择空闲空间较多的磁盘
        sort(candidates.begin(), candidates.end(), [](Disk* a, Disk* b) {
            return a->used_capcity < b->used_capcity;
        });
    }
    if (candidates.size() < REP_NUM) {
        cerr << "false!" << endl;
        return false;
    }
    auto obj = new Object(id,size,tag);
    // 为每个副本分配空间
    // TODO: 分区满后的写入仍有优化空间
    for (int i = 0; i < REP_NUM; ++i) {
        Disk* target_disk = candidates[i];
        std::vector<int> target_units;
        int start = target_disk->next_free_unit[tag];

        for(int j = 0; j < size; ++j){
            while (target_disk->units[start].obj_id != -1){
                ++start;
                if(start > target_disk->capacity) start = 1;
            }
            target_units.push_back(start);
            target_disk->insert(start, id, j, tag);
            ++start;
            if (start > target_disk->capacity) start = 1;
        }

        obj->replica_allocate(target_disk->id, target_units);
    }

    objects[id] = obj;
    return true;
}

void StorageController::write_action(){
    // recieve input
    int n_write;
    cin >> n_write;
    vector<tuple<int, int, int>> write_ops;
    for(int i=0; i<n_write; ++i) {
        int id, size, tag;
        cin >> id >> size >> tag;
        write_ops.emplace_back(id, size, tag);
    }

    for(auto& [id, size, tag] : write_ops) {
            bool success = write_object(id, size, tag);
            if(!success) {
                // this branch should not be triggered
                cerr << "Write failed for object " << id << endl;
                exit(1);
            }
            
            auto obj = objects[id];
            cout << obj->id << endl;
            for(auto& rep : obj->replicas) {
                cout << rep.disk_id;
                for(int u : rep.units)  cout << " " << u;
                cout << endl;
            }
        }
        cout.flush();
}

void StorageController::process_read_request(int req_id, int obj_id) {
    auto obj_it = objects.find(obj_id);
    if (obj_it == objects.end()) return;
    auto req = new ReadRequest(req_id, current_time, obj_it->second);
    requests[req_id] = req;
    obj_it->second->active_requests.insert(req_id);
}
    


vector<string> StorageController::generate_disk_actions() {
    vector<string> actions;
    actions.reserve(disks.size());
    for (auto disk : disks) {
        
        int tokens = G; 
        string act_str;
        act_str.reserve(64); // 预估动作串长度，减少扩容
        
        // 判断是否采用 Jump 策略（仅在时间片初始允许）
        int jump_target = -1;
        const int search_limit = G; // 向前搜索上限
        int steps = 0;
        int pos = disk->head_pos;
        while (steps <= search_limit) {
            if (disk->units[pos].obj_id != -1) {
                Unit &u = disk->units[pos];
                auto obj_it = objects.find(u.obj_id);
                if (obj_it != objects.end()) {
                    bool needed = false;
                    // 检查该对象是否有活跃请求且该块还未读取
                    for (int req_id : obj_it->second->active_requests) {
                        auto req_it = requests.find(req_id);
                        if (req_it != requests.end()){
                            if (!req_it->second->block_read[u.obj_offset]) {
                                needed = true;
                                break;
                            }
                        }
                    }
                    if (needed) break;
                }
            }
            pos++;
            if (pos > disk->capacity) pos = 1;
            steps++;
        }
        // 如果空闲区间超过阈值（例如G个单位），则采用 Jump
        if (steps >= G && steps <= search_limit && tokens == G) {
            jump_target = pos;
        }
        if(jump_target != -1) {
            act_str = "j " + to_string(jump_target);
            tokens = 0; // Jump后本时间片内不再有动作
            // 更新 head_pos 为 jump_target 后的下一个位置
            disk->head_pos = jump_target;
            disk->pre_tokens = 0;
            actions.push_back(move(act_str));
        } else {
        
            // 如果上一个时间片有 read 动作，则根据其令牌消耗计算首次 read 消耗
            int last_read_cost = (disk->pre_tokens > 0) ? disk->pre_tokens : 0;
            
            while (tokens > 0) {
                int pos = disk->head_pos;
                // auto it = disk->used_units.find(pos);
                Unit *it = disk->units + pos;
                if (it->obj_id != -1) {
                    // 当前有对象块，计划执行 Read 操作
                    int cost = (last_read_cost == 0) ? 64 : max(16, int(ceil(float(last_read_cost) * 0.8) ));
                    if (tokens >= cost) {
                        act_str.push_back('r');
                        tokens -= cost;
                        last_read_cost = cost;  // 更新当前 Read 消耗
                        
                        Unit *u = it; // 当前读取的单元
                        int obj_id = u->obj_id;
                        int blk_idx = u->obj_offset;
                        auto obj_it = objects.find(obj_id);
                        if (obj_it != objects.end()) {
                            Object *obj = obj_it->second;
                            // 遍历所有活跃该对象的读请求，更新增量计数器
                            for (int req_id : obj->active_requests) {
                                auto req_it = requests.find(req_id);
                                if (req_it != requests.end()){
                                    ReadRequest *req = req_it->second;
                                    // 如果该块第一次被读到，则更新计数器
                                    if (!req->block_read[blk_idx]) {
                                        req->block_read[blk_idx] = true;
                                        req->unique_blocks_read++;
                                    }
                                    // 检查是否完成
                                    auto obj_it = objects.find(req->obj_id);
                                    if (obj_it != objects.end()) {
                                        Object *obj = obj_it->second;
                                        if (req->unique_blocks_read >= obj->size) {
                                            pending_completed_requests.insert(req->req_id);
                                        }
                                    }
                                    // 仍然保留原有 read_blocks 记录（如果有其他用途）
                                    req->read_blocks[disk->id].insert(blk_idx);
                                }
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    // 没有对象块则执行 Pass 动作，消耗 1 个令牌
                    if (tokens >= 1) {
                        act_str.push_back('p');
                        tokens -= 1;
                        last_read_cost = 0;  // 重置连续 Read 消耗记录
                    } else {
                        break;
                    }
                }
                // 更新磁头位置（环形排列）
                disk->head_pos++;
                if(disk->head_pos>disk->capacity) {
                    disk->head_pos = 1;
                }
            }
            act_str.push_back('#');
            // 如果本时间片最后一个动作为 Read，则记录其令牌消耗；否则置 0
            if (act_str.size() > 1 && act_str[act_str.size()-2] == 'r')
                disk->pre_tokens = last_read_cost;
            else
                disk->pre_tokens = 0;
            actions.push_back(move(act_str));
        }
    }
    return actions;
}
    
    
vector<int> StorageController::check_completed_requests() {
    vector<int> completed;
    for (int req_id : pending_completed_requests) {
        auto req_it = requests.find(req_id);
        if (req_it == requests.end()) continue; // 请求已被处理或取消

        ReadRequest* req = req_it->second;
        auto obj_it = objects.find(req->obj_id);
        if (obj_it == objects.end()) {
            // 对象已删除，标记为完成
            completed.push_back(req_id);
            requests.erase(req_id);
        } else {
            Object *obj = obj_it->second;
            if (req->unique_blocks_read >= obj->size) {
                completed.push_back(req_id);
                obj->active_requests.erase(req_id);
                requests.erase(req_id);
            }
        }
    }
    pending_completed_requests.clear();
    return completed;
}


void StorageController::read_action(){
    // recieve input
    int n_read;
    cin >> n_read;
    vector<pair<int, int>> read_ops;
    for(int i=0; i<n_read; ++i) {
        int req_id, obj_id;
        cin >> req_id >> obj_id;
        read_ops.emplace_back(req_id, obj_id);
    }

    for(auto& [req_id, obj_id] : read_ops)
            process_read_request(req_id, obj_id);

    auto actions = generate_disk_actions();
    for(string& act : actions)
        cout << act << endl;
    
    auto completed = check_completed_requests();
    cout << completed.size() << endl;
    for(int id : completed) cout << id << endl;
    cout.flush();
}
