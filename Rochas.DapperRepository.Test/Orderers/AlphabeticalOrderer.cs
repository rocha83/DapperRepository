using System;
using System.Collections.Generic;
using System.Linq;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Rochas.DapperRepository.Test
{
    public class AlphabeticalOrderer : ITestCaseOrderer
    {
        public IEnumerable<TTestCase> OrderTestCases<TTestCase>(
            IEnumerable<TTestCase> testCases) where TTestCase : ITestCase =>
            testCases.OrderBy(testCase => testCase.TestMethod.Method.Name);
    }
}