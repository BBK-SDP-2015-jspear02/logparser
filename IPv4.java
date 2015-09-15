package code;

import sun.net.util.IPAddressUtil;

/**
 * This class handles any operations related to IPv4 address.
 */
public class IPv4 implements IP{
    private String ipAddress;

    public IPv4(String ipAddress){
        if ((ipAddress == null) || (ipAddress.equals("") || (!IPAddressUtil.isIPv4LiteralAddress(ipAddress)))) {
            throw new IllegalArgumentException("The ip address field is empty!");
        }
        this.ipAddress = ipAddress;
        getIpNumber();
    }
    /**
     * @return result The ip number as a long translated from an IP Address
     */
    public long getIpNumber() {
        String[] ipAddressInArray = this.ipAddress.split("\\.");
        long result = 0;
        long ip = 0;
        for (int x = 3; x >= 0; x--) {
            ip = Long.parseLong(ipAddressInArray[3 - x]);
            result |= ip << (x << 3);
        }

        return result;
    }

}
