class agent {
    const inet  ip;
    const int   beta;
    const int   mu;
    KEY(ip);
};

class destination {
    const inet  ip;
    const int   gamma;
    const int   mu;
    KEY(ip);
};

class traceroute {
    const agent       source;
    const destination destination;
    const timestamp   ts;
    const int         alpha;
    const int         alpha1;
    KEY(source, destination, ts);
};

class ip {
    const inet ip;
    const int delta;
    const int delta1;
    KEY(ip);
};
