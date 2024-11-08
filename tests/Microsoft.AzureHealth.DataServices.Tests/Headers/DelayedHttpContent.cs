using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.AzureHealth.DataServices.Headers;

/// <summary>
/// Purpose of this mock httpContext to mimic situation when creation of httprequest content takes a lot of time.
/// </summary>
/// <param name="content">content</param>
/// <param name="miliseconds">how long to delay before creating content</param>
public class DelayedHttpContent(string content, int miliseconds = 1000) : HttpContent
{
    protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
    {
        await Task.Delay(miliseconds);
        stream.Write(Encoding.UTF8.GetBytes(content));
    }

    protected override bool TryComputeLength(out long length)
    {
        length = content.Length;
        return true;
    }
}
