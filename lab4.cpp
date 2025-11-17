#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <fstream>
#include <random>
#include <chrono>
#include <thread>
#include <shared_mutex> 
#include <mutex>
#include <algorithm>
#include <iomanip>

enum class OpType { READ, WRITE, STRING };

struct Op {
    OpType type;
    int idx;   
    int value; 
};

class MultiField {
public:
    explicit MultiField(size_t m) : vals(m, 0), locks(m) {}

    int read(size_t idx) const {
        if (idx >= vals.size()) return 0;
        std::shared_lock<std::shared_mutex> lk(locks[idx]);
        return vals[idx];
    }

    void write(size_t idx, int value) {
        if (idx >= vals.size()) return;
        std::unique_lock<std::shared_mutex> lk(locks[idx]);
        vals[idx] = value;
    }

    std::string to_string() const {
        std::vector<std::shared_lock<std::shared_mutex>> acquired_locks;
        acquired_locks.reserve(locks.size());
        for (auto& mtx : locks) {
            acquired_locks.emplace_back(mtx);
        }

        std::ostringstream oss;
        oss << "{";
        for (size_t i = 0; i < vals.size(); ++i) {
            if (i > 0) oss << ", ";
            oss << vals[i];
        }
        oss << "}";
        return oss.str();
    }

    operator std::string() const {
        return to_string();
    }

private:
    std::vector<int> vals;
    mutable std::vector<std::shared_mutex> locks;
};

std::vector<Op> load_ops(const std::string& filename) {
    std::ifstream ifs(filename);
    std::vector<Op> ops;
    if (!ifs.is_open()) {
        std::cerr << "Error opening file: " << filename << std::endl;
        return ops;
    }
    std::string cmd;
    while (ifs >> cmd) {
        if (cmd == "read") {
            int idx; ifs >> idx;
            ops.push_back({ OpType::READ, idx, 0 });
        }
        else if (cmd == "write") {
            int idx, val; ifs >> idx >> val;
            ops.push_back({ OpType::WRITE, idx, val });
        }
        else if (cmd == "string") {
            ops.push_back({ OpType::STRING, 0, 0 });
        }
    }
    return ops;
}

void worker(MultiField& data, const std::vector<Op>& ops) {
    for (const auto& op : ops) {
        switch (op.type) {
        case OpType::READ:
            data.read(op.idx);
            break;
        case OpType::WRITE:
            data.write(op.idx, op.value);
            break;
        case OpType::STRING: {
            std::string s = data;
            volatile size_t len = s.length();
            (void)len;
            break;
        }
        }
    }
}

std::mt19937 rng(std::random_device{}());
void generate_variant6_files(size_t count, int thread_idx) {
    std::string fname = "var6_t" + std::to_string(thread_idx) + ".txt";
    std::ofstream ofs(fname);
    std::discrete_distribution<int> dist({ 20, 5, 20, 5, 20, 5, 25 });
    std::uniform_int_distribution<int> val_dist(1, 100);

    for (size_t i = 0; i < count; ++i) {
        int action = dist(rng);
        switch (action) {
        case 0: ofs << "read 0\n"; break;
        case 1: ofs << "write 0 " << val_dist(rng) << "\n"; break;
        case 2: ofs << "read 1\n"; break;
        case 3: ofs << "write 1 " << val_dist(rng) << "\n"; break;
        case 4: ofs << "read 2\n"; break;
        case 5: ofs << "write 2 " << val_dist(rng) << "\n"; break;
        case 6: ofs << "string\n"; break;
        }
    }
}

void generate_uniform_files(size_t count, int thread_idx, int m) {
    std::string fname = "uniform_t" + std::to_string(thread_idx) + ".txt";
    std::ofstream ofs(fname);
    std::uniform_int_distribution<int> field_dist(0, m - 1);
    std::uniform_int_distribution<int> type_dist(0, 2); 
    std::uniform_int_distribution<int> val_dist(1, 100);

    for (size_t i = 0; i < count; ++i) {
        int t = type_dist(rng);
        if (t == 0) ofs << "read " << field_dist(rng) << "\n";
        else if (t == 1) ofs << "write " << field_dist(rng) << " " << val_dist(rng) << "\n";
        else ofs << "string\n";
    }
}

void generate_skewed_files(size_t count, int thread_idx) {
    std::string fname = "skewed_t" + std::to_string(thread_idx) + ".txt";
    std::ofstream ofs(fname);
    std::uniform_int_distribution<int> val_dist(1, 100);

    for (size_t i = 0; i < count; ++i) {
        if (rng() % 100 < 90) {
            ofs << "write 0 " << val_dist(rng) << "\n";
        }
        else {
            ofs << "string\n";
        }
    }
}

void run_test(const std::string& case_name, const std::string& file_prefix, int num_threads, MultiField& data) {
    std::vector<std::vector<Op>> thread_ops(num_threads);

    for (int i = 0; i < num_threads; ++i) {
        thread_ops[i] = load_ops(file_prefix + "_t" + std::to_string(i) + ".txt");
    }

    auto start = std::chrono::steady_clock::now();

    std::vector<std::thread> workers;
    for (int i = 0; i < num_threads; ++i) {
        workers.emplace_back(worker, std::ref(data), std::cref(thread_ops[i]));
    }

    for (auto& t : workers) t.join();

    auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> diff = end - start;

    std::cout << "Case: " << std::setw(10) << case_name
        << "Threads: " << num_threads
        << "Time: " << diff.count() << " s" << std::endl;
}

int main() {
    const int M = 3; 
    const size_t OPS_PER_THREAD = 100000;
    const int MAX_THREADS = 3;

    std::cout << "Generating Files\n";
    for (int i = 0; i < MAX_THREADS; ++i) {
        generate_variant6_files(OPS_PER_THREAD, i);
        generate_uniform_files(OPS_PER_THREAD, i, M);
        generate_skewed_files(OPS_PER_THREAD, i);
    }
    std::cout << "Files generated.\n\n";

    std::cout << "Starting Measurements\n";

    for (int t = 1; t <= MAX_THREADS; ++t) {
        MultiField data(M);
        run_test("Variant 6", "var6", t, data);
    }
    std::cout << "\n";

    for (int t = 1; t <= MAX_THREADS; ++t) {
        MultiField data(M);
        run_test("Uniform", "uniform", t, data);
    }
    std::cout << "\n";

    for (int t = 1; t <= MAX_THREADS; ++t) {
        MultiField data(M);
        run_test("Skewed", "skewed", t, data);
    }

    return 0;
}