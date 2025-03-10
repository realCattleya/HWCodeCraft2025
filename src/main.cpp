#include<bits/stdc++.h>

using namespace std;

#define MAX_DISK_NUM (10)
#define MAX_DISK_SIZE (16384)
#define MAX_REQUEST_NUM (30000000)
#define MAX_OBJECT_NUM (100000)
#define REP_NUM (3)
#define FRE_PER_SLICING (1800)
#define EXTRA_TIME (105)

typedef struct Unit_{
    int unit_id;
    int obj_id;
    int obj_offset;
} Unit;

typedef struct Disk_ {
    int id;
    int capacity;
    int head_pos = 1;
    int pre_tokens;
    string last_action;
    unordered_map<int,Unit> used_units;

    Disk_(int id, int v) : id(id), capacity(v) {}
} Disk;

typedef struct ObjectReplica_ {
    int disk_id;
    vector<int> units;
} ObjectReplica;

typedef struct Object_ {
    int id;
    int size;
    int tag;
    bool is_delete;
    vector<ObjectReplica> replicas;
    set<int> active_requests;
} Object;

typedef struct ReadRequest_ {
    int req_id;
    int obj_id;
    int start_time;
    bool isdone;
    unordered_map<int, set<int>> read_blocks; // disk_id -> blocks
} ReadRequest;

class StorageController {
private:
    int T; // time slices
    int M; // total tags number
    int N; // total disks number
    int V; // units number per disk
    int G; // tokens budget per disk
    vector<Disk> disks;
    unordered_map<int, Object> objects;
    unordered_map<int, ReadRequest> requests;
    int current_time = 0;
    
    // pre-process data
    vector<vector<int>> fre_del;
    vector<vector<int>> fre_write;
    vector<vector<int>> fre_read;

public:
    StorageController(int T, int M, int N, int V, int G,
                    vector<vector<int>>& del,
                    vector<vector<int>>& write,
                    vector<vector<int>>& read)
        : T(T), M(M), N(N), V(V), G(G),
          fre_del(del), fre_write(write), fre_read(read) {
        for(int i=1; i<=N; ++i)
            disks.emplace_back(i, V);
    }

    void pre_process(){
        //TODO: preprocess
        cout << "OK" << endl;
        cout.flush();
    }

    void timestamp_align(){
        string cmd;
        cin >> cmd >> current_time;
        assert(cmd == "TIMESTAMP");
        cout << "TIMESTAMP " << current_time << endl;
        cout.flush();
    }

    vector<int> handle_delete(const vector<int>& obj_ids){
        vector<int> aborted;
        for(int obj_id : obj_ids) {
            auto it = objects.find(obj_id);
            if(it == objects.end()) continue;

            // erase objs disk units
            for(auto& rep : it->second.replicas) {
                Disk& disk = disks[rep.disk_id-1];
                for(int u : rep.units){
                    auto uit = disk.used_units.find(u);
                    disk.used_units.erase(uit);
                }
            }

            // cancel corresponding requests
            for(int req_id : it->second.active_requests) {
                aborted.push_back(req_id);
                requests.erase(req_id);
            }
            
            objects.erase(it);
        }
        return aborted;
    }

    void delete_action(){
        // recieve input
        int n_del;
        cin >> n_del;
        vector<int> del_objs(n_del);
        for(int i=0; i<n_del; ++i) cin >> del_objs[i];

        // handle_delete
        auto aborted = handle_delete(del_objs);
            cout << aborted.size() << endl;
            for(int id : aborted) cout << id << endl;
            cout.flush();
    }


    bool write_object(int id, int size, int tag){
        // choose candidates disks
        vector<Disk*> candidates;
        for(auto& d : disks) {
            if((int)d.used_units.size() + size <= V)
                candidates.push_back(&d);
        }
        if(candidates.size() < REP_NUM) return false;

        // trival stragety
        sort(candidates.begin(), candidates.end(), [](Disk* a, Disk* b) {
            return a->used_units.size() < b->used_units.size();
        });

        Object obj;
        obj.id = obj_id;
        obj.size = size;
        obj.tag = tag;
        obj.is_delete = false;

        // allocate units
        for(int i=0; i<3; ++i) {
            Disk* target_disk = candidates[i];
            //first find free units
            ObjectReplica rep;
            rep.disk_id = target_disk->id;
            int start = 1;
            for(int j=0; j<size; ++j) {
                while(target_disk->used_units.find(start)!=target_disk->used_units.end()){
                    start++;
                }
                rep.units.push_back(start);
                Unit unit = {start,obj_id,j};
                target_disk->used_units[start] = unit;
            }
            obj.replicas.push_back(rep);
        }

        objects[obj_id] = obj;
        return true;
    }

    void write_action(){
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
                
                auto& obj = objects[id];
                cout << obj.id << endl;
                for(auto& rep : obj.replicas) {
                    cout << rep.disk_id;
                    for(int u : rep.units) cout << " " << u;
                    cout << endl;
                }
            }
            cout.flush();
    }

    void process_read_request(int req_id, int obj_id) {
        auto it = objects.find(obj_id);
        if(it == objects.end()) return;
        
        ReadRequest req;
        req.req_id = req_id;
        req.obj_id = obj_id;
        req.start_time = current_time;
        requests[req_id] = req;
        it->second.active_requests.insert(req_id);
    }


    vector<string> generate_disk_actions() {
        
    }

    vector<int> check_completed_requests() {
        
    }

    void read_action(){
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

};

int main() {
    // 读取全局输入
    int T, M, N, V, G;
    cin >> T >> M >> N >> V >> G;

    auto read_matrix = [](int rows) {
        vector<vector<int>> mat(rows);
        for(int i=0; i<rows; ++i) {
            int cnt = (T+FRE_PER_SLICING-1)/FRE_PER_SLICING; // ceil(T/1800)
            mat[i].resize(cnt);
            for(int j=0; j<cnt; ++j)
                cin >> mat[i][j];
        }
        return mat;
    };

    auto fre_del = read_matrix(M);
    auto fre_write = read_matrix(M);
    auto fre_read = read_matrix(M);

    StorageController controller(T, M, N, V, G, fre_del, fre_write, fre_read);
    controller.pre_process();

    for (int t=1; t<=T+EXTRA_TIME; t++) {
        controller.timestamp_align();
        controller.delete_action();
        controller.write_action();
        controller.read_action();
    }
    
    return 0;
}

