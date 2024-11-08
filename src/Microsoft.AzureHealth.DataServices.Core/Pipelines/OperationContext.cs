using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AzureHealth.DataServices.Clients.Headers;
using Microsoft.AzureHealth.DataServices.Protocol;

namespace Microsoft.AzureHealth.DataServices.Pipelines
{
    /// <summary>
    /// Operation context used for input and output for a filter in a pipeline.
    /// </summary>
    public class OperationContext
    {
        private readonly Dictionary<string, string> properties;
        private readonly IHttpCustomHeaderCollection headers;

        /// <summary>
        /// Creates an instance of OperationContext
        /// </summary>
        public OperationContext()
        {
            properties = new Dictionary<string, string>();
            headers = new HttpCustomHeaderCollection();
        }

        /// <summary>
        /// Creates an instance of OperationContext
        /// </summary>
        /// <param name="message">Initial http request message</param>
        /// <param name="headers">Initial header collection</param>
        private OperationContext(HttpRequestMessage message, IHttpCustomHeaderCollection headers)
        {
            _ = message ?? throw new ArgumentNullException(nameof(message));

            Request = message;
            properties = new Dictionary<string, string>();
            this.headers = headers;
        }

        /// <summary>
        /// Gets a dictionary of custom properties.
        /// </summary>
        public Dictionary<string, string> Properties { get => properties; }

        /// <summary>
        /// Gets or sets an http request.
        /// </summary>
        public HttpRequestMessage Request { get; set; }

        /// <summary>
        /// Gets or sets the status code for an http response.
        /// </summary>
        public HttpStatusCode StatusCode { get; set; }

        /// <summary>
        /// Gets or sets the body content of an http request or response.
        /// </summary>
        public byte[] Content { get; set; }

        /// <summary>
        /// Gets the collection of headers. Request headers feed into bindings. Response headers feed into the output of the pipeline.
        /// </summary>
        public IHttpCustomHeaderCollection Headers { get => headers; }

        /// <summary>
        /// Gets or sets the body content of an http request or response as a string.
        /// </summary>
        public string ContentString
        {
            get => Content is not null ? System.Text.Encoding.UTF8.GetString(Content) : string.Empty;
            set => Content = value != null ? System.Text.Encoding.UTF8.GetBytes(value) : null;
        }

        /// <summary>
        /// Gets or sets an indicator that processing of the operation context has a fatal error.
        /// </summary>
        public bool IsFatal { get; set; }

        /// <summary>
        /// Gets or sets an exception when a fatal error occurs processing the operation context
        /// </summary>
        public Exception Error { get; set; }

        public static async Task<OperationContext> CreateAsync(HttpRequestMessage message) =>
            await CreateAsync(message, new HttpCustomHeaderCollection());

        public static async Task<OperationContext> CreateAsync(HttpRequestMessage message, IHttpCustomHeaderCollection headers)
        {
            OperationContext context = new(message, headers);
            await context.SetContentAsync(message);
            return context;
        }

        /// <summary>
        /// Updates the request URI and method.
        /// </summary>
        /// <param name="method">HTTP method, e.g., GET, POST, PUT, DELETE</param>
        /// <param name="routePrefix">Route prefix of path, e.g., fhir.</param>
        /// <param name="resource">Optional FHIR resource type.</param>
        /// <param name="id">Optional FHIR Id</param>
        /// <param name="operation">Optional FHIR operation</param>
        /// <param name="version">Optional FHIR version</param>
        public void UpdateFhirRequestUri(HttpMethod method, string routePrefix = null, string resource = null, string id = null, string operation = null, string version = null)
        {
            FhirUriPath requestPath = new(method, Request.RequestUri, routePrefix);
            requestPath.Resource = resource ?? requestPath.Resource;
            requestPath.Id = id ?? requestPath.Id;
            requestPath.Operation = operation ?? requestPath.Operation;
            requestPath.Version = version ?? requestPath.Version;

            UriBuilder uriBuilder = new(Request.RequestUri)
            {
                Path = requestPath.Path,
            };
            Request.RequestUri = uriBuilder.Uri;

            Request.Method = method;
        }

        /// <summary>
        /// Updates a generic request URI and method.
        /// </summary>
        /// <param name="method">HTTP method, e.g., GET, POST, PUT, DELETE</param>
        /// <param name="baseUrl">Base url of the request exclusive of path or query.</param>
        /// <param name="path">Optional path.</param>
        /// <param name="query">Optional query.</param>
        public void UpdateRequestUri(HttpMethod method, string baseUrl, string path = null, string query = null)
        {
            UriBuilder uriBuilder = new(baseUrl)
            {
                Path = path,
                Query = query,
            };
            Request.RequestUri = uriBuilder.Uri;
            Request.Method = method;
        }

        private async Task SetContentAsync(HttpRequestMessage message)
        {
            long? contentLength = message.Content?.Headers.ContentLength;
            if (!(contentLength == null || contentLength == 0))
            {
                Content = await message.Content.ReadAsByteArrayAsync();
            }
        }
    }
}
