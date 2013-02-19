class ip {
    const inet ip;
    const int delta;
    const int delta2;
    KEY(ip);
};

class hop {
    const ip   ip;
    const int  ttl;
};

class traceroute {
    const ip          source;
    const ip          destination;
    const int         alpha;
    const int         alpha2;
    const hop         hops[];
    const timestamp   ts;
    KEY(source, destination, ts);
};


