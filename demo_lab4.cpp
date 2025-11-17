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

enum class OpType { READ, WRITE, STRING };

struct Op {
    OpType type;
    size_t idx; 
    int value;  
};

class MultiField {
public:
    MultiField(size_t m, int init_value = 0)
        : vals(m, init_value), locks(m) {
    }

    int read(size_t idx) {
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
        std::vector<std::shared_lock<std::shared_mutex>> acquired;
        acquired.reserve(locks.size());
        for (auto& lk : locks) {
            acquired.emplace_back(lk);
        }
        std::ostringstream oss;
        oss << "[";
        for (size_t i = 0; i < vals.size(); ++i) {
            if (i) oss << ", ";
            oss << vals[i];
        }
        oss << "]";
        return oss.str();
    }

    operator std::string() const {
        return to_string();
    }

    size_t size() const { return vals.size(); }

private:
    std::vector<int> vals;
    mutable std::vector<std::shared_mutex> locks;
};

std::vector<Op> load_ops_from_file(const std::string& filename) {
    std::ifstream ifs(filename);
    std::vector<Op> ops;
    if (!ifs) {
        std::cerr << "Cannot open file: " << filename << "\n";
        return ops;
    }
    std::string cmd;
    while (ifs >> cmd) {
        if (cmd == "read") {
            size_t idx; ifs >> idx;
            ops.push_back({ OpType::READ, idx, 0 });
        }
        else if (cmd == "write") {
            size_t idx; int val; ifs >> idx >> val;
            ops.push_back({ OpType::WRITE, idx, val });
        }
        else if (cmd == "string") {
            ops.push_back({ OpType::STRING, 0, 0 });
        }
        else {
            std::string rest; std::getline(ifs, rest);
        }
    }
    return ops;
}

void execute_ops(MultiField& mf, const std::vector<Op>& ops) {
    for (const auto& op : ops) {
        switch (op.type) {
        case OpType::READ:
            mf.read(op.idx);
            break;
        case OpType::WRITE:
            mf.write(op.idx, op.value);
            break;
        case OpType::STRING:
        {
            volatile std::size_t dummy = std::string(mf).size();
            (void)dummy;
        }
        break;
        }
    }
}

std::mt19937_64 rng(std::random_device{}());

void generate_files_matching_distribution(
    size_t m,
    const std::vector<double>& read_weights,
    const std::vector<double>& write_weights,
    double string_prob,
    size_t total_ops,
    size_t threads,
    const std::string& prefix)
{
    std::discrete_distribution<size_t> read_dist(read_weights.begin(), read_weights.end());
    std::discrete_distribution<size_t> write_dist(write_weights.begin(), write_weights.end());
    std::uniform_real_distribution<double> prob01(0.0, 1.0);
    std::uniform_int_distribution<int> val_dist(1, 1000);

    size_t ops_per_file = total_ops / threads;
    for (size_t t = 0; t < threads; ++t) {
        std::ostringstream name;
        name << prefix << "_thread" << t << ".txt";
        std::ofstream ofs(name.str());
        for (size_t i = 0; i < ops_per_file; ++i) {
            double p = prob01(rng);
            if (p < string_prob) {
                ofs << "string\n";
            }
            else {
                double choose_rw = prob01(rng);
                if (choose_rw < 0.5) {
                    size_t idx = read_dist(rng);
                    ofs << "read " << idx << "\n";
                }
                else {
                    size_t idx = write_dist(rng);
                    int val = val_dist(rng);
                    ofs << "write " << idx << " " << val << "\n";
                }
            }
        }
        ofs.close();
        std::cout << "Generated " << name.str() << " (" << ops_per_file << " ops)\n";
    }
}

void generate_uniform_files(size_t m, size_t total_ops, size_t threads, const std::string& prefix) {
    std::uniform_int_distribution<int> val_dist(1, 1000);
    std::uniform_int_distribution<size_t> field_dist(0, m - 1);
    size_t ops_per_file = total_ops / threads;
    for (size_t t = 0; t < threads; ++t) {
        std::ostringstream name; name << prefix << "_thread" << t << ".txt";
        std::ofstream ofs(name.str());
        for (size_t i = 0; i < ops_per_file; ++i) {
            int r = rng() % 3;
            if (r == 0) {
                size_t idx = field_dist(rng);
                ofs << "read " << idx << "\n";
            }
            else if (r == 1) {
                size_t idx = field_dist(rng);
                int val = val_dist(rng);
                ofs << "write " << idx << " " << val << "\n";
            }
            else {
                ofs << "string\n";
            }
        }
        ofs.close();
        std::cout << "Generated (uniform) " << name.str() << "\n";
    }
}

void generate_skewed_files(size_t m, size_t total_ops, size_t threads, const std::string& prefix) {
    std::uniform_int_distribution<int> val_dist(1, 1000);
    size_t ops_per_file = total_ops / threads;
    for (size_t t = 0; t < threads; ++t) {
        std::ostringstream name; name << prefix << "_thread" << t << ".txt";
        std::ofstream ofs(name.str());
        for (size_t i = 0; i < ops_per_file; ++i) {
            double p = (rng() % 100) / 100.0;
            if (p < 0.7) {
                ofs << "read 0\n";
            }
            else if (p < 0.85) {
                ofs << "write 0 " << val_dist(rng) << "\n";
            }
            else {
                size_t idx = 1 + (rng() % (m > 1 ? m - 1 : 1));
                if (rng() % 2 == 0) ofs << "read " << idx << "\n"; else ofs << "write " << idx << " " << val_dist(rng) << "\n";
            }
            if ((rng() % 1000) == 0) ofs << "string\n";
        }
        ofs.close();
        std::cout << "Generated (skewed) " << name.str() << "\n";
    }
}

void run_test_case(const std::vector<std::string>& files, size_t m) {
    std::vector<std::vector<Op>> all_ops;
    all_ops.reserve(files.size());
    for (auto& f : files) {
        all_ops.push_back(load_ops_from_file(f));
        std::cout << "File " << f << " -> " << all_ops.back().size() << " ops (loaded)\n";
    }

    MultiField mf(m, 0);

    auto t0 = std::chrono::steady_clock::now();

    std::vector<std::thread> threads;
    for (size_t i = 0; i < all_ops.size(); ++i) {
        threads.emplace_back([&mf, &ops = all_ops[i]]() {
            execute_ops(mf, ops);
            });
    }
    for (auto& th : threads) if (th.joinable()) th.join();

    auto t1 = std::chrono::steady_clock::now();
    double secs = std::chrono::duration_cast<std::chrono::duration<double>>(t1 - t0).count();
    std::cout << "Execution with " << files.size() << " threads finished in " << secs << " s\n";

    std::cout << "Final state (first 10 fields): ";
    std::string s = std::string(mf);
    std::cout << s.substr(0, std::min<size_t>(s.size(), 200)) << (s.size() > 200 ? "..." : "") << "\n";
}

int main() {
    size_t m = 16;
    size_t total_ops = 200000; 
    size_t threads_options[] = { 1, 2, 3 };

    std::vector<double> read_weights(m, 1.0), write_weights(m, 1.0);
    read_weights[0] = 8.0; write_weights[0] = 2.0; 
    read_weights[1] = 1.0; write_weights[1] = 6.0; 
    double string_prob = 0.05;

    std::cout << "Generating files for case (a) - matching distribution\n";
    generate_files_matching_distribution(m, read_weights, write_weights, string_prob, total_ops, 3, "case_a");

    std::cout << "Generating files for case (b) - uniform distribution\n";
    generate_uniform_files(m, total_ops, 3, "case_b");

    std::cout << "Generating files for case (c) - skewed distribution\n";
    generate_skewed_files(m, total_ops, 3, "case_c");

    std::vector<std::string> files_a = { "case_a_thread0.txt", "case_a_thread1.txt", "case_a_thread2.txt" };
    std::vector<std::string> files_b = { "case_b_thread0.txt", "case_b_thread1.txt", "case_b_thread2.txt" };
    std::vector<std::string> files_c = { "case_c_thread0.txt", "case_c_thread1.txt", "case_c_thread2.txt" };

    for (size_t thr : threads_options) {
        std::cout << "=== Running measurements for " << thr << " thread(s) — case (a) ===\n";
        std::vector<std::string> fs(files_a.begin(), files_a.begin() + thr);
        run_test_case(fs, m);

        std::cout << "=== Running measurements for " << thr << " thread(s) — case (b) ===\n";
        fs.assign(files_b.begin(), files_b.begin() + thr);
        run_test_case(fs, m);

        std::cout << "=== Running measurements for " << thr << " thread(s) — case (c) ===\n";
        fs.assign(files_c.begin(), files_c.begin() + thr);
        run_test_case(fs, m);
    }

    std::cout << "Done.\n";
    return 0;
}
