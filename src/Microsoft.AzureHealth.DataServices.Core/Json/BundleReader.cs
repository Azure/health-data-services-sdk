﻿using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace Microsoft.AzureHealth.DataServices.Json
{
    /// <summary>
    /// FHIR bundle reader.
    /// </summary>
    public class BundleReader : JObjectReader
    {
        private readonly bool _ifNoneExist;

        /// <summary>
        /// Creates a new instance of BundleReader.
        /// </summary>
        /// <param name="root">The root object to read.</param>
        /// <param name="ifNoneExist">FHIR ifNoneExists flag omits if false.</param>
        public BundleReader(JObject root, bool ifNoneExist)
            : base(root)
        {
            _ifNoneExist = ifNoneExist;
        }

        /// <summary>
        /// Gets the bundle enumerator.
        /// </summary>
        /// <returns>Enumerator of JToken if exists, otherwise null.</returns>
        public override IEnumerator<JToken> GetEnumerator()
        {
            if (Root.IsArray("$.entry"))
            {
                JArray entries = (JArray)Root["entry"];
                return new BundleEnumerator(entries, _ifNoneExist);
            }
            else
            {
                return null;
            }
        }
    }
}
