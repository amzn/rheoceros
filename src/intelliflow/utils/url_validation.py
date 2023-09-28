# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from ipaddress import ip_address
from logging import critical, info
from urllib.parse import urlparse

from validators import domain


def validate_url(url):
    """
    Validate URL using the validation script provided by mitigation guide
    Refuse URL with IP address in domain section: https://skb.highcastle.a2z.com/guides/127
    """
    url_object = urlparse(url)
    try:
        if url_object.port is not None:
            if url_object.netloc.split(":")[0].lower() == "localhost":
                critical("Potential SSRF attack by providing an localhost in URL")
                return False
            elif ip_address(url_object.netloc.split(":")[0]):
                critical("Potential SSRF attack by providing an IP address in URL.")
                return False
        else:
            if url_object.netloc.lower() == "localhost":
                critical("Potential SSRF attack by providing an localhost in URL.")
                return False
            elif ip_address(url_object.netloc):
                critical("Potential SSRF attack by providing an IP address in URL.")
                return False
    except ValueError:
        if url_object.port is not None and domain(url_object.netloc.split(":")[0]):
            info("SSRF validation: Domain in" + url + "is valid.")
            return True
        elif domain(url_object.netloc):
            info("SSRF validation: Domain in" + url + "is valid.")
            return True
        else:
            critical("SSRF validation: invalid domain name")
            return False
