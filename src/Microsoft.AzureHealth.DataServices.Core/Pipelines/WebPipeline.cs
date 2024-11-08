using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.ApplicationInsights;
using Microsoft.AzureHealth.DataServices.Bindings;
using Microsoft.AzureHealth.DataServices.Channels;
using Microsoft.AzureHealth.DataServices.Clients;
using Microsoft.AzureHealth.DataServices.Clients.Headers;
using Microsoft.AzureHealth.DataServices.Filters;
using Microsoft.Extensions.Logging;

namespace Microsoft.AzureHealth.DataServices.Pipelines
{
    /// <summary>
    /// A custom operation pipeline for a ASPNET Web API.
    /// </summary>
    public class WebPipeline : IPipeline<HttpRequestMessage, HttpResponseMessage>
    {
        private readonly string _name;
        private readonly string _id;
        private readonly IHttpCustomHeaderCollection _headers;
        private readonly IInputFilterCollection _inputFilters;
        private readonly IOutputFilterCollection _outputFilters;
        private readonly IBinding _binding;
        private readonly IInputChannelCollection _inputChannels;
        private readonly IOutputChannelCollection _outputChannels;
        private readonly TelemetryClient _telemetryClient;
        private readonly ILogger logger;
        private OperationContext _context;
        private long _startTicks;

        /// <summary>
        /// Creates an instance of WebPipeline.
        /// </summary>
        /// <param name="inputFilters">Optional collection of input filters.</param>
        /// <param name="inputChannels">Optional cCollection of input channels.</param>
        /// <param name="binding">Optional binding. </param>
        /// <param name="outputFilters">Optional collection of output filters.</param>
        /// <param name="outputChannels">Optional collection of output channels.</param>
        /// <param name="headers">Optional collection of custom headers for OperationContext</param>
        /// <param name="telemetryClient">Optional application insights telemetry client.</param>
        /// <param name="logger">Optional ILogger.</param>
        public WebPipeline(IInputFilterCollection inputFilters = null, IInputChannelCollection inputChannels = null, IBinding binding = null, IOutputFilterCollection outputFilters = null, IOutputChannelCollection outputChannels = null, IHttpCustomHeaderCollection headers = null, TelemetryClient telemetryClient = null, ILogger<WebPipeline> logger = null)
            : this("WebPipeline", Guid.NewGuid().ToString(), inputFilters, inputChannels, binding, outputFilters, outputChannels, headers, telemetryClient, logger)
        {
        }

        internal WebPipeline(IInputFilterCollection inputFilters = null, IInputChannelCollection inputChannels = null, IBinding binding = null, IOutputFilterCollection outputFilters = null, IOutputChannelCollection outputChannels = null, IHttpCustomHeaderCollection headers = null, TelemetryClient telemetryClient = null, ILogger<AzureFunctionPipeline> logger = null)
            : this("WebPipeline", Guid.NewGuid().ToString(), inputFilters, inputChannels, binding, outputFilters, outputChannels, headers, telemetryClient, logger)
        {
        }

        internal WebPipeline(string name, string id, IInputFilterCollection inputFilters = null, IInputChannelCollection inputChannels = null, IBinding binding = null, IOutputFilterCollection outputFilters = null, IOutputChannelCollection outputChannels = null, IHttpCustomHeaderCollection headers = null, TelemetryClient telemetryClient = null, ILogger logger = null)
        {
            _name = name;
            _id = id;
            _headers = headers ?? new HttpCustomHeaderCollection();
            _inputFilters = inputFilters ?? new InputFilterCollection();
            _outputFilters = outputFilters ?? new OutputFilterCollection();
            _inputChannels = inputChannels ?? new InputChannelCollection();
            _outputChannels = outputChannels ?? new OutputChannelCollection();
            _binding = binding;
            _telemetryClient = telemetryClient;
            this.logger = logger;

            foreach (IFilter inputFilter in _inputFilters)
            {
                inputFilter.OnFilterError += InputFilter_OnFilterError;
            }

            foreach (IFilter outputFilter in _outputFilters)
            {
                outputFilter.OnFilterError += OutputFilter_OnFilterError;
            }

            foreach (IChannel inputChannel in _inputChannels)
            {
                inputChannel.OnError += InputChannel_OnError;
            }

            foreach (IChannel outputChannel in _outputChannels)
            {
                outputChannel.OnError += OutputChannel_OnError;
            }

            if (binding != null)
            {
                binding.OnError += Binding_OnError;
                binding.OnComplete += Binding_OnComplete;
            }
        }

        /// <summary>
        /// Signals an event that an error occurred in the pipeline.
        /// </summary>
        public event EventHandler<PipelineErrorEventArgs> OnError;

        /// <summary>
        /// Signals an event when the pipeline completes.
        /// </summary>
        public event EventHandler<PipelineCompleteEventArgs> OnComplete;

        /// <summary>
        /// Gets the name of the pipeline.
        /// </summary>
        public string Name => _name;

        /// <summary>
        /// Gets the unique ID of the pipeline instance.
        /// </summary>
        public string Id => _id;

        /// <summary>
        /// Executes the pipeline and returns a response for the caller.
        /// </summary>
        /// <param name="request">Iniitial request from the Web service.</param>
        /// <returns>Response for Web service.</returns>
        public async Task<HttpResponseMessage> ExecuteAsync(HttpRequestMessage request)
        {
            _startTicks = DateTime.Now.Ticks;

            try
            {
                _context = await OperationContext.CreateAsync(request, _headers);

                _context = await ExecuteFiltersAsync(_inputFilters, _context);

                await ExecuteChannelsAsync(_inputChannels, _context);

                _context = await ExecuteBindingAsync(_binding, _context);

                _context = await ExecuteFiltersAsync(_outputFilters, _context);

                await ExecuteChannelsAsync(_outputChannels, _context);

                _context.StatusCode = !_context.IsFatal && _context.StatusCode == 0 ? HttpStatusCode.OK : _context.StatusCode;
                HttpResponseMessage response = new(_context.StatusCode);

                response.Content = !string.IsNullOrEmpty(_context.ContentString) ? new ByteArrayContent(_context.Content) : null;

                foreach (var errorHeader in response.AddCustomHeadersToResponse(_headers))
                {
                    logger?.LogInformation($"Could not add header {errorHeader.Name} with value {errorHeader.Value} due to validation error.");
                }

                logger?.LogInformation("Pipeline {Name}-{Id} complete {ExecutionTime}ms", Name, Id, TimeSpan.FromTicks(DateTime.Now.Ticks - _startTicks).TotalMilliseconds);
                OnComplete?.Invoke(this, new PipelineCompleteEventArgs(Id, Name, _context));

                return response;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Pipeline {Name}-{Id} error with fault response.", Name, Id);
                _telemetryClient?.TrackException(ex);
                OnError?.Invoke(this, new PipelineErrorEventArgs(Id, Name, ex));
            }

            logger?.LogWarning("Pipeline {Name}-{Id} fault return 503 response.", Name, Id);
            return new HttpResponseMessage(HttpStatusCode.InternalServerError);
        }

        private async Task<OperationContext> ExecuteBindingAsync(IBinding binding, OperationContext context)
        {
            if (context.IsFatal || binding == null)
            {
                return context;
            }

            return await binding.ExecuteAsync(context);
        }

        private async Task<OperationContext> ExecuteFiltersAsync(IFilterCollection filters, OperationContext context)
        {
            foreach (IFilter filter in filters)
            {
                try
                {
                    if (ExecutionStatusCheck(filter.ExecutionStatusType, context))
                    {
                        logger?.LogInformation("Pipeline {Name}-{Id} filter {FilterName}-{FilterId} executed with status {Status}.", Name, Id, filter.Name, filter.Id, filter.ExecutionStatusType);
                        context = await filter.ExecuteAsync(context);
                    }
                    else
                    {
                        logger?.LogInformation("Pipeline {Name}-{Id} filter {FilterName}-{FilterId} not executed due to status {Status}.", Name, Id, filter.Name, filter.Id, filter.ExecutionStatusType);
                    }
                }
                catch (Exception ex)
                {
                    _telemetryClient?.TrackException(ex);
                    logger?.LogError(ex, "Pipeline {Name}-{Id} channel {FilterName}-{FilterName} error.", Name, Id, filter.Name, filter.Id);
                    context.IsFatal = true;
                    context.StatusCode = HttpStatusCode.InternalServerError;
                    context.Error = ex;
                }
            }

            return context;
        }

        private async Task ExecuteChannelsAsync(IChannelCollection channels, OperationContext context)
        {
            foreach (IChannel channel in channels)
            {
                try
                {
                    if (ExecutionStatusCheck(channel.ExecutionStatusType, context))
                    {
                        if (channel.State == ChannelState.None)
                        {
                            logger?.LogInformation("Pipeline {Name}-{Id} opening channel {ChannelName}-{ChannelId} first time.", Name, Id, channel.Name, channel.Id);
                            await channel.OpenAsync();
                        }

                        if (channel.State != ChannelState.Open)
                        {
                            logger?.LogInformation("Pipeline {Name}-{Id} channel {ChannelName}-{ChannelId} is not open, will try to close and open.", Name, Id, channel.Name, channel.Id);
                            await channel.CloseAsync();
                            await channel.OpenAsync();
                        }

                        logger?.LogInformation("Pipeline {Name}-{Id} channel {ChannelName}-{ChannelId} is in state {State}.", Name, Id, channel.Name, channel.Id, channel.State);

                        var contentType = context.Request.Content?.Headers.ContentType?.ToString();
                        contentType ??= context.Request.Headers.Accept.ToString();

                        await channel.SendAsync(context.Content, new object[] { contentType });
                        logger?.LogInformation("Pipeline {Name}-{Id} channel {ChannelName}-{ChannelId} sent message.", Name, Id, channel.Name, channel.Id);
                    }
                    else
                    {
                        logger?.LogInformation("Pipeline {Name}-{Id} channel {ChannelName}-{ChannelId} not executed due to status {Status}.", Name, Id, channel.Name, channel.Id, channel.ExecutionStatusType);
                    }
                }
                catch (Exception ex)
                {
                    _telemetryClient?.TrackException(ex);
                    logger?.LogError(ex, "Pipeline {Name}-{Id} channel {ChannelName}-{ChannelId} error.", Name, Id, channel.Name, channel.Id);
                    context.IsFatal = true;
                    context.StatusCode = HttpStatusCode.InternalServerError;
                    context.Error = ex;
                }
            }
        }

        private bool ExecutionStatusCheck(StatusType status, OperationContext context)
        {
            return (status == StatusType.Fault && context.IsFatal) ||
                   (status == StatusType.Normal && !context.IsFatal) ||
                   (status == StatusType.Any);
        }

        #region error events

        private void Binding_OnComplete(object sender, BindingCompleteEventArgs e)
        {
            logger?.LogInformation("Pipeline {Name}-{Id} binding {BindingName}-{BindingId} completed.", Name, Id, e.Name, e.Id);
        }

        private void Binding_OnError(object sender, BindingErrorEventArgs e)
        {
            _context.IsFatal = true;
            _context.StatusCode = HttpStatusCode.InternalServerError;
            _context.Error = e.Error;
            logger?.LogError(e.Error, "Pipeline {Name}-{Id} binding {BindingName}- {BindingId} error.", Name, Id, e.Name, e.Id);
            OnError?.Invoke(this, new PipelineErrorEventArgs(Id, Name, e.Error));
        }

        private void OutputChannel_OnError(object sender, ChannelErrorEventArgs e)
        {
            logger?.LogError(e.Error, "Pipeline {Name}-{Id} output channel {ChannelName}- {ChannelId} error.", Name, Id, e.Name, e.Id);
            _context.IsFatal = true;
            _context.StatusCode = HttpStatusCode.InternalServerError;
            _context.Error = e.Error;
            OnError?.Invoke(this, new PipelineErrorEventArgs(Id, Name, e.Error));
        }

        private void InputChannel_OnError(object sender, ChannelErrorEventArgs e)
        {
            logger?.LogError(e.Error, "Pipeline {Name}-{Id} input channel {ChannelName}- {ChannelId} error.", Name, Id, e.Name, e.Id);
            _context.IsFatal = true;
            _context.StatusCode = HttpStatusCode.InternalServerError;
            _context.Error = e.Error;
            OnError?.Invoke(this, new PipelineErrorEventArgs(Id, Name, e.Error));
        }

        private void OutputFilter_OnFilterError(object sender, FilterErrorEventArgs e)
        {
            _context.IsFatal = true;
            _context.StatusCode = e.Code ?? HttpStatusCode.InternalServerError;
            _context.Error = e.Error;
            logger?.LogError(e.Error, "Pipeline {Name}-{Id} output filter {FilterName}-{FilterId} error.", Name, Id, e.Name, e.Id);
            OnError?.Invoke(this, new PipelineErrorEventArgs(Id, Name, e.Error));
        }

        private void InputFilter_OnFilterError(object sender, FilterErrorEventArgs e)
        {
            _context.IsFatal = true;
            _context.StatusCode = e.Code ?? HttpStatusCode.InternalServerError;
            _context.Error = e.Error;
            logger?.LogError(e.Error, "Pipeline {Name}-{Id} input filter {FilterName}-{FilterId} error.", Name, Id, e.Name, e.Id);
            OnError?.Invoke(this, new PipelineErrorEventArgs(Id, Name, e.Error));
        }

        #endregion
    }
}
