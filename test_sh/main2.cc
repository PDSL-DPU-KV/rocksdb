#include <cstdint>
#include <fstream>
#include <iostream>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

struct BenchmarkResult {
  std::string benchmarkName;
  double throughput;  // 每秒操作数
  int p90Latency;
  int p99Latency;
  int p999Latency;
};

std::vector<BenchmarkResult> parseBenchmarkResults(
    const std::string& filePath) {
  std::vector<BenchmarkResult> results;
  std::ifstream file(filePath);
  if (!file) {
    std::cerr << "无法打开文件: " << filePath << std::endl;
    return results;
  }

  std::string line;
  std::regex benchmarkRegex(
      "(fillseq|fillrandom|ycsb_load|ycsb_run|mixgraph)\\s*:\\s*(\\d+\\.\\d+)\\s+micros/"
      "op\\s+(\\d+)\\s+ops/sec.*");
  std::regex latencyRegex("p90,(\\d+),p99,(\\d+),p999,(\\d+),p9999,.*");

  BenchmarkResult currentResult;
  bool inBenchmarkSection = false;

  while (std::getline(file, line)) {
    std::smatch match;
    if (std::regex_search(line, match, benchmarkRegex)) {
      if (inBenchmarkSection) {
        results.push_back(currentResult);
      }
      currentResult.benchmarkName = match[1];
      currentResult.throughput = std::stod(match[3]);
      inBenchmarkSection = true;
    } else if (std::regex_search(line, match, latencyRegex)) {
      currentResult.p90Latency = std::stoi(match[1]);
      currentResult.p99Latency = std::stoi(match[2]);
      currentResult.p999Latency = std::stoi(match[3]);
    }
  }

  // 添加最后一个结果
  if (inBenchmarkSection) {
    results.push_back(currentResult);
  }

  file.close();
  return results;
}

void saveResultsToFile(const std::vector<BenchmarkResult>& results,
                       const std::string& outputFilePath) {
  std::ofstream outputFile(outputFilePath);
  if (!outputFile) {
    std::cerr << "无法打开输出文件: " << outputFilePath << std::endl;
    return;
  }

  outputFile << "基准测试名称,吞吐量 (ops/sec),P90延迟 (micros),P99延迟 "
                "(micros),P999延迟 (micros)\n";
  for (const auto& result : results) {
    outputFile << std::fixed << result.benchmarkName << ", \n"
               << (uint64_t)result.throughput << ", " << result.p90Latency << ", "
               << result.p99Latency << ", " << result.p999Latency << "\n";
  }

  outputFile.close();
}

int main() {
  std::string filePath = "out.out";  // 替换为实际的文件名
  std::vector<BenchmarkResult> results = parseBenchmarkResults(filePath);
  std::string outputFilePath = "output.csv";  // 替换为你想要的输出文件名
  saveResultsToFile(results, outputFilePath);

  std::cout.setf(std::ios::fixed);
  for (const auto& result : results) {
    std::cout << "基准测试名称: " << result.benchmarkName << std::endl;
    std::cout << std::fixed << "吞吐量 (ops/sec): " << (uint64_t)result.throughput
              << std::endl;
    std::cout << "P90延迟 (micros): " << result.p90Latency << std::endl;
    std::cout << "P99延迟 (micros): " << result.p99Latency << std::endl;
    std::cout << "P999延迟 (micros): " << result.p999Latency << std::endl;
    std::cout << "------------------------" << std::endl;
  }

  return 0;
}