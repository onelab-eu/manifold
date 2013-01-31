class traceroute {
    const ip        source;
    const ip        destination;
    const int       alpha;
    const int       alpha2;
    const timestamp ts;
    KEY(source, destination, ts);
};

class ip {
    const inet ip;
    const int delta;
    const int delta2;
    KEY(ip);
};
