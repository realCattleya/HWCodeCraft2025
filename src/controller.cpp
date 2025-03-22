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
    vector<pair<int,long long>> tags_size_sum;
    for (int tag = 1; tag <= M; ++tag) {
        for (int t = 0; t < time_intervals; ++t) {
            long long total_write = fre_write[tag - 1][t];
            long long total_read = fre_read[tag - 1][t];
            // 计算热度：读取量 / (写入量 + 1)，防止除零
            tag_hotness[tag][t] = (double)total_read / (total_write + 1);
            if(t==0){
                pair<int,long long> p(tag,fre_write[tag - 1][0]-fre_del[tag-1][0]);
                tags_size_sum.push_back(p);
                continue;
            }
            tags_size_sum[tag-1].second += fre_write[tag - 1][t]-fre_del[tag-1][t];
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
    // 注意：所有对ReadRequest删除的操作都在这里完成，别的地方不要删否则会导致剩下的无法释放
    //      如果request对应的object被删了就把target设置为nullptr
    while (true)
    {
        auto it = requests.find(last_not_expire_req_id);
        if (it != requests.end())
        {
            if (it->second->expire_time < current_time)
            {
                auto req = it->second;
                if (req->target != nullptr && req->target->active_requests.find(req) != req->target->active_requests.end()) {
                    req->target->active_requests.erase(req);
                    req->target->invalid_requests.push_back(req->req_id);
                }
                delete req;
                requests.erase(it);
                last_not_expire_req_id += 1;
            } else {
                break;
            }
        } else {
            break;
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
        for(auto req : it->second->active_requests) {
            req->target = nullptr;
            aborted.push_back(req->req_id);
        }
        for(int req_id : it->second->invalid_requests) {
            aborted.push_back(req_id);
        }
        objects.erase(it);
        delete it->second;
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
        target_units = target_disk->next_free_unit(size,tag);
        sort(target_units.begin(),target_units.end());
        obj->replica_allocate(target_disk->id, target_units);
        for(int j = 0; j < size; ++j){
            target_disk->insert(target_units[j], id, j, tag);
        }
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
    obj_it->second->active_requests.insert(req);
}

float compute_score(Object *obj, int offset, int ts, Disk *disk, const std::vector<int> visited) {
    std::unordered_set<int> offsets;
    for (int v: visited) {
        if (disk->units[v].obj_id == obj->id) {
            offsets.insert(disk->units[v].obj_offset);
        }
    }
    return obj->get_score(offset, ts, offsets);   
}

vector<string> StorageController::generate_disk_actions() {
    vector<string> actions;
    actions.reserve(disks.size());
    for (auto &disk : disks) {
        
        int tokens = G; 
        string act_str;
        act_str.reserve(64); // 预估动作串长度，减少扩容
        
        // --- Jump 策略 ---
        int jump_target = -1;
        const int search_limit = G / 2; // 向前搜索上限
        int steps = 0;
        int pos = disk->head_pos;
        while (steps <= G + 100) {
            if (disk->units[pos].obj_id != -1) {
                Unit &u = disk->units[pos];
                auto obj_it = objects.find(u.obj_id);
                if (obj_it != objects.end()) {
                    bool needed = false;
                    // 检查该对象是否有活跃请求且该块还未读取
                    for (auto req : obj_it->second->active_requests) {
                        if (!req->block_read[u.obj_offset]) {
                            needed = true;
                            break;
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
        if (steps >= search_limit && tokens == G) {
            jump_target = pos;
        }
        // jump_target = pos;
        if(jump_target != -1) {
            act_str = "j " + to_string(jump_target);
            tokens = 0; // Jump后本时间片内不再有动作
            // 更新 head_pos 为 jump_target 后的下一个位置
            disk->head_pos = jump_target;
            disk->pre_tokens = 0;
            actions.push_back(move(act_str));
            continue;
        }

        // --- DP PR 策略 ---
        vector<double> dp(G + 1, -1e9); // 初始化 DP 数组，最大令牌数为 G，初始状态为负无穷
        vector<string> action_types(G + 1, "");  // 用来存储每个令牌数的操作类型（Read/Pass）
        vector<int> pre_read_costs(G + 1, 0);  // 用来存储每个令牌数的 Read 操作消耗
        vector<int> dp_pos(G + 1, 0);  // 用来存储每个状态的磁头位置
        vector<vector<int>> dp_read_pos(G + 1, vector<int>(G + 1, 0));  // 用来存储每个状态的 Read 操作的磁头位置
        dp[G] = 0; // 初始状态下，消耗 0 个令牌时的得分为 0
        int last_read_cost = (disk->pre_tokens > 0) ? disk->pre_tokens : 0;  // 上一个时间片的 `Read` 消耗
        pre_read_costs[G] = last_read_cost;  // 初始化最大令牌数的 Read 消耗
        dp_pos[G] = disk->head_pos;

        // 状态转移：遍历每个令牌数 tokens_left
        for (int tokens_left = G; tokens_left > 0; tokens_left--) {  // 从 G 到 0 反向更新 DP
            // Pass 操作（不增加得分，只消耗 1 个令牌）
            if (tokens_left > 0 && dp[tokens_left] != -1e9) {
                dp[tokens_left - 1] = max(dp[tokens_left] + 0, dp[tokens_left - 1]);
                if (dp[tokens_left] + 0 == dp[tokens_left - 1]) {
                    action_types[tokens_left - 1] = "p";  // 记录 Pass 操作
                    pre_read_costs[tokens_left - 1] = 0;
                    dp_pos[tokens_left - 1] = dp_pos[tokens_left] + 1;
                    if (dp_pos[tokens_left - 1] > disk->capacity) {
                        dp_pos[tokens_left - 1] = 1;
                    }
                }
            }
            // Read 操作（如果当前位置有对象块，计算得分）
            if (tokens_left >= pre_read_costs[tokens_left] && dp[tokens_left] != -1e9) {
                Unit *it = disk->units + dp_pos[tokens_left];
                if (it->obj_id != -1) {
                    int obj_id = it->obj_id;
                    Object* obj = objects[obj_id];
                    int cost = (pre_read_costs[tokens_left] == 0) ? 64 : max(16, int(ceil(float(pre_read_costs[tokens_left]) * 0.8) ));
                    if (tokens_left - cost >= 0) {
                        // TODO: DP STATUS
                        dp[tokens_left - cost] = max(dp[tokens_left] + compute_score(obj, it->obj_offset, current_time, disk, dp_read_pos[tokens_left]), dp[tokens_left - cost]); //FIXME: compute_score非常耗费资源，此处强制要求拷贝，以防影响原有的信息维护
                        if (dp[tokens_left] + compute_score(obj, it->obj_offset, current_time, disk, dp_read_pos[tokens_left]) == dp[tokens_left - cost]) {
                            action_types[tokens_left - cost] = "r";  // 记录 Read 操作
                            pre_read_costs[tokens_left - cost] = cost;
                            dp_pos[tokens_left - cost] = dp_pos[tokens_left] + 1;
                            copy(dp_read_pos[tokens_left].begin(), dp_read_pos[tokens_left].end(), dp_read_pos[tokens_left - cost].begin());
                            dp_read_pos[tokens_left - cost].push_back(dp_pos[tokens_left]);
                            if (dp_pos[tokens_left - cost] > disk->capacity) {
                                dp_pos[tokens_left - cost] = 1;
                            }
                        }
                    }
                } else {
                    int cost = (pre_read_costs[tokens_left] == 0) ? 64 : max(16, int(ceil(float(pre_read_costs[tokens_left]) * 0.8) ));
                    if (tokens_left - cost >= 0) {
                        dp[tokens_left - cost] = max(dp[tokens_left] + 0, dp[tokens_left - cost]);
                        if (dp[tokens_left] + 0 == dp[tokens_left - cost]) {
                            action_types[tokens_left - cost] = "r";  // 无对象块时，依然记录 Read 操作
                            pre_read_costs[tokens_left - cost] = cost;
                            dp_pos[tokens_left - cost] = dp_pos[tokens_left] + 1;
                            if (dp_pos[tokens_left - cost] > disk->capacity) {
                                dp_pos[tokens_left - cost] = 1;
                            }
                        }
                    }
                }
            }
        }

        // 选择最大得分，并反向回溯生成操作路径
        int tokens_left = 0;  // 从 0 令牌数开始回溯

        // 用于存储所有操作
        vector<string> temp_actions;
        bool found_read = false;

        while (tokens_left < G && action_types[tokens_left] != "") {
            if (action_types[tokens_left] == "r") {
                // 记录 Read 操作
                act_str = "r";
                temp_actions.push_back(act_str);
                if (!found_read){
                    disk->pre_tokens = (pre_read_costs[tokens_left] == 0) ? 64 : pre_read_costs[tokens_left]; // max(16, int(ceil(float(pre_read_costs[tokens_left]) * 0.8) ));
                }
                found_read = true;  // 标记找到第一个 Read
            } else if (action_types[tokens_left] == "p") {
                // 记录 Pass 操作
                act_str = "p";
                if (found_read) {
                    temp_actions.push_back(act_str);
                }
            }

            if (action_types[tokens_left] == "r") {
                last_read_cost = pre_read_costs[tokens_left];  // 记录最后一个 Read 消耗
                tokens_left += last_read_cost;  // 减少令牌消耗
            } else if (action_types[tokens_left] == "p") {
                tokens_left += 1;  // Pass 操作消耗 1 个令牌
            }
        }

        // 如果没有找到 Read 操作，则强制将最后一个操作设为 Read
        if (!found_read) {
            act_str = "r";
            temp_actions.push_back(act_str);
            last_read_cost = (disk->pre_tokens == 0) ? 64 : max(16, int(ceil(float(disk->pre_tokens) * 0.8) ));
            disk->pre_tokens = last_read_cost;
        }

        // 反转操作路径以保证顺序
        reverse(temp_actions.begin(), temp_actions.end());
        
        // actions.insert(actions.end(), temp_actions.begin(), temp_actions.end());

        // 检查完成的请求
        for (const auto &s: temp_actions) {
            if (s == "r") {
                Unit &u = disk->units[disk->head_pos];
                if (u.obj_id != -1)
                {
                    Object *obj = objects[u.obj_id];
                    for (auto it = obj->active_requests.begin(); it != obj->active_requests.end();) {
                        ReadRequest *rq = *it;
                        if (!rq->block_read[u.obj_offset]) {
                            rq->block_read[u.obj_offset] = true;
                            rq->unique_blocks_read += 1;
                        }
                        if (rq->unique_blocks_read == obj->size) {
                            rq->isdone = true;
                            pending_completed_requests.insert(rq);
                            it = obj->active_requests.erase(it);
                        } else {
                            it ++;
                        }
                        
                    }
                    
                }
            }
            disk->head_pos += 1;
            if (disk->head_pos > disk->capacity) {
                disk->head_pos = 1;
            }
        }
        // disk->head_pos += temp_actions.size();
        // if (disk->head_pos > disk->capacity) {
        //     disk->head_pos = 1;
        // }

        temp_actions.push_back("#");
        // FIXME: 检查一下输出这里
        std::string act = "";
        for (const auto& s: temp_actions) {
            act += s;
        }
        actions.push_back(move(act));
    }
    return actions;
}    
    
vector<int> StorageController::check_completed_requests() {
    vector<int> completed;
    for (auto req : pending_completed_requests) {
        req->target = nullptr;
        auto obj_it = objects.find(req->obj_id);
        if (obj_it == objects.end()) {
            // 对象已删除，标记为完成
            completed.push_back(req->req_id);
        } else {
            Object *obj = obj_it->second;
            if (req->unique_blocks_read >= obj->size) {
                completed.push_back(req->req_id);
                obj->active_requests.erase(req);
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
