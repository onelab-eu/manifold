class ip {
    const inet ip;
    const unsigned asn; 
    KEY(ip);
};

class agent {
    const ip ip;
    const text sonoma_agent;
    KEY(ip);
};
