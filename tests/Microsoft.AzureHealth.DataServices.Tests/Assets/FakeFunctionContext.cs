﻿using System;
using System.Collections.Generic;
using Microsoft.Azure.Functions.Worker;

namespace Microsoft.AzureHealth.DataServices.Tests.Assets
{
    public class FakeFunctionContext : FunctionContext, IDisposable
    {
        private readonly FunctionInvocation invocation;

        public FakeFunctionContext()
            : this(new FakeFunctionDefinition(), new FakeFunctionInvocation())
        {
        }

        public FakeFunctionContext(FunctionDefinition functionDefinition, FunctionInvocation invocation)
        {
            FunctionDefinition = functionDefinition;
            this.invocation = invocation;
        }

        public override RetryContext RetryContext => throw new NotImplementedException();

        public bool IsDisposed { get; private set; }

        public override IServiceProvider InstanceServices { get; set; }

        public override FunctionDefinition FunctionDefinition { get; }

        public override IDictionary<object, object> Items { get; set; }

        public override IInvocationFeatures Features { get; } = null;

        public override string InvocationId => invocation.Id;

        public override string FunctionId => invocation.FunctionId;

        public override TraceContext TraceContext => invocation.TraceContext;

        public override BindingContext BindingContext { get; }

        public void Dispose()
        {
            if (!IsDisposed)
            {
                IsDisposed = true;
                GC.SuppressFinalize(this);
            }
        }
    }
}
