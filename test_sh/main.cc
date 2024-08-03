#include <cstdint>
#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <sstream>

using namespace std;

int main() {
    double average_sum[3] = { 0,0 ,0 };
    for (int index = 0;index < 8;++index) {
        string input = "cpu-" + to_string(index) + ".log";
        ifstream input_file(input);
        string line;
        int size = 0;
        vector<double> db_bench, rocksdb_high, rocksdb_low;
        while (getline(input_file, line)) {
            istringstream iss(line);
            string key;
            double value;
            iss >> key;
            if (key == "COMMAND") {
                db_bench.push_back(0.0);
                rocksdb_high.push_back(0.0);
                rocksdb_low.push_back(0.0);
                while (getline(input_file, line)) {
                    istringstream iss2(line);
                    iss2 >> key;
                    if (key == "top") break;
                    iss2 >> value;
                    if (key == "db_bench") {
                        db_bench[size] += value;
                    }
                    else if (key == "rocksdb:high") {
                        rocksdb_high[size] += value;
                    }
                    else if (key == "rocksdb:low") {
                        rocksdb_low[size] += value;
                    }
                }
                if (db_bench[size] != 0 || rocksdb_high[size] != 0 || rocksdb_low[size] != 0)
                    size++;
            }
        }
        input_file.close();
        string output = "cpu-" + to_string(index) + ".csv";
        ofstream output_file(output);
        output_file << " , " << "db_bench, "
            << "rocksdb:high, "
            << "rocksdb:low\n";
        double max[3] = { 0,0,0 }, aver[3] = { 0,0,0 };
        for (int i = 0; i < size; ++i) {
            output_file << i + 1 << ", " << db_bench[i] << ", " << rocksdb_high[i] << ", "
                << rocksdb_low[i] << endl;
            max[0] = std::max(max[0], db_bench[i]);
            max[1] = std::max(max[1], rocksdb_high[i]);
            max[2] = std::max(max[2], rocksdb_low[i]);
            aver[0] += db_bench[i] / size;
            aver[1] += rocksdb_high[i] / size;
            aver[2] += rocksdb_low[i] / size;
        }
        output_file << "max, " << max[0] << ", " << max[1] << "," << max[2] << std::endl;
        output_file << "average, " << aver[0] << ", " << aver[1] << ", " << aver[2] << std::endl;
        for (int m = 0;m < 3;++m)
            average_sum[m] += aver[m];
        output_file << "average_sum, " << average_sum[0] << ", " << average_sum[1] << ", " << average_sum[2] << std::endl;
        output_file.close();
    }
    return 0;
}
