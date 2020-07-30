﻿using HouseofCat.Library.Logger;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Workflow
{
    public class DataflowEngine<TIn, TOut>
    {
        private readonly ILogger<DataflowEngine<TOut, TIn>> _logger;
        private readonly ActionBlock<TIn> _block;
        private readonly Func<TIn, Task<TIn>> _preWorkBody;
        private readonly Func<TIn, Task<TOut>> _workBody;
        private readonly Func<TOut, Task> _postWorkBody;

        public DataflowEngine(
            Func<TIn, Task<TOut>> workBody,
            int maxDegreeOfParallelism,
            bool ensureOrdered,
            Func<TIn, Task<TIn>> preWorkBody = null,
            Func<TOut, Task> postWorkBody = null)
        {
            _logger = LogHelper.GetLogger<DataflowEngine<TOut, TIn>>();
            _workBody = workBody ?? throw new ArgumentNullException(nameof(workBody));

            _preWorkBody = preWorkBody;
            _postWorkBody = postWorkBody;

            _block = new ActionBlock<TIn>(
                ExecuteWorkBodyAsync,
                new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = maxDegreeOfParallelism,
                    EnsureOrdered = ensureOrdered
                });
        }

        private async Task ExecuteWorkBodyAsync(TIn data)
        {
            try
            {
                if (_preWorkBody != null)
                {
                    data = await _preWorkBody(data).ConfigureAwait(false);
                }

                if (_postWorkBody != null)
                {
                    var output = await _workBody(data).ConfigureAwait(false);
                    if (output != null)
                    {
                        await _postWorkBody(output).ConfigureAwait(false);
                    }
                }
                else
                {
                    await _workBody(data).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Execption occurred in the Dataflow Engine.");
            }
        }

        public async ValueTask EnqueueWorkAsync(TIn data)
        {
            await _block
                .SendAsync(data)
                .ConfigureAwait(false);
        }
    }
}
