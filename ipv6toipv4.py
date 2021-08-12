import ipaddress


def convertusingipaddress(ipv4address):
    print(ipaddress.IPv6Address('0000:0000:0000:0000:0000:ffff:' + ipv4address))



convertusingipaddress("46.56.67.92")
convertusingipaddress("168.0.108.20")


# 0000:0000:0000:0000:0000:ffff:a800:6c14,0000:0000:0000:0000:0000:ffff:2e38:435c