'use strict';

export async function runAndReport(name, worker) {
  if (global.gc) {
    global.gc();
  }
  const start = process.hrtime.bigint();
  await worker();
  const report = process.report.getReport();

  // Similar to process.report.writeReport() output
  //   console.log(JSON.stringify(report, null, 2));
  console.info(
    '================== Benchmarking %s ========================',
    name
  );
  console.info(
    'Run on Node.JS %s [%s - %s]',
    report.header.nodejsVersion,
    report.header.arch,
    report.header.platform
  );
  console.log(
    `Benchmark took\t\t\t${(process.hrtime.bigint() - start) / 1000000n} ms`
  );
  console.info(
    'JavaScript Heap Used Memory\t%d MB',
    Math.round((report.javascriptHeap.usedMemory / 1024 / 1024) * 100) / 100
  );
  console.info(
    'User CPU seconds:\t\t%d\r\nKernel CPU seconds:\t\t%d',
    report.resourceUsage.userCpuSeconds,
    report.resourceUsage.kernelCpuSeconds
  );
  //   console.info(
  //     'Filesystem activity:\t\t%d reads,\t\t\t%d writes',
  //     report.resourceUsage.fsActivity.reads,
  //     report.resourceUsage.fsActivity.writes
  //   );
}
