class node {
    const unsigned cpu;         /**< Free CPU */
    const unsigned bw;          /**< Bandwidth utilization */
    const inet     ip;
    const unsigned load;        /**< The average 5-minute load (as reported by the Unix uptime command) over the selected period */
    const unsigned reliability; /**< CoMon queries nodes every 5 minutes, for 255 queries per day. The average reliability is the percentage of queries over the selected period for which CoMon reports a value. The period is the most recent for which data is available, with CoMon data being collected by MySlice daily*/
    const unsigned mem;         /**< The average active memory utilization as reported by CoMon */
    const bool     ssh;         /**< The average response delay of the node to SSH logins over the selected period for which CoMon reports a value. The period is the most recent for which data is available, with CoMon data being collected by MySlice daily */
    const unsigned slices;      /**< Average number of active slices over the selected period for which CoMon reports a value. The period is the most recent for which data is available, with CoMon data being collected by MySlice daily */
    const text     hostname;
};
