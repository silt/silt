/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include "print.h"

void bytes_into_hex_string(const u_char *data, u_int len, string& dststr)
{
    static const char hexes[] = "0123456789ABCDEF";
    dststr.reserve(dststr.size() + len*2);

    for (u_int i = 0; i < len; i++) {
        dststr.push_back(hexes[data[i] >> 4]);
        dststr.push_back(hexes[data[i] & 0xf]);
    }
}

/* May incur a copy;  don't use on high-performance path unless you know
 * str is refcounted */
string bytes_to_hex(const u_char* data, u_int len)
{
    string r;
    bytes_into_hex_string(data, len, r);
    return r;
}

string bytes_to_hex(const string& s)
{
    return bytes_to_hex((const u_char *)s.data(), s.size());
}


int get_digit_value(char digit)
{
    if (isdigit(digit))
	{
	    return digit - '0';
	}
    else if (digit >= 'A' && digit <= 'F')
	{
	    return digit - 'A' + 10;
	}
    else if (digit >= 'a' && digit <= 'f')
	{
	    return digit - 'a' + 10;
	}
    else  // illegal digit
	{
	    return -1;
	}
}

// assumes 2 character string with legal hex digits
string* hex_string_to_bytes(const u_char* hex_num, u_int len)
{
    string* p_ret = new string();
    for (u_int i = 0; i < len/2; i++) {
        char high = hex_num[i];
        char low  = hex_num[i+1];
        int low_val = get_digit_value(low);   //convert low  to number from 0 to 15
        int high_val = get_digit_value(high); //convert high to number from 0 to 15

        if ((low_val < 0) || (high_val < 0)) {
            // Narf!
            delete p_ret;
            return NULL;
        }

        char ch = low_val + 16 * high_val;
        p_ret->append(1, ch);
    }
    return p_ret;
}


/*
 * print data in rows of 16 bytes: offset   hex   ascii
 *
 * 00000   47 45 54 20 2f 20 48 54  54 50 2f 31 2e 31 0d 0a   GET / HTTP/1.1..
 */
void print_hex_ascii_line(const u_char *payload,
                          u_int len,
                          u_int offset)
{
    /* offset */
    fprintf(stderr, "%05d   ", offset);

    /* hex */
    const u_char *ch = payload;
    for (u_int i = 0; i < len; i++) {
        fprintf(stderr, "%02x ", *ch);
        ch++;
        /* print extra space after 8th byte for visual aid */
        if (i == 7)
            fprintf(stderr, " ");
    }
    /* print space to handle line less than 8 bytes */
    if (len < 8)
        fprintf(stderr, " ");

    /* fill hex gap with spaces if not full line */
    if (len < 16) {
        int gap = 16 - len;
        for (int i = 0; i < gap; i++) {
            fprintf(stderr, "   ");
        }
    }
    fprintf(stderr, "   ");

    /* ascii (if printable) */
    ch = payload;
    for (u_int i = 0; i < len; i++) {
        if (isprint(*ch))
            fprintf(stderr, "%c", *ch);
        else
            fprintf(stderr, ".");
        ch++;
    }

    fprintf(stderr, "\n");

    return;
}

void print_payload(const string &str, int indent) {
    print_payload((const u_char*)str.data(), str.size(), indent);
}
/*
 * print packet payload data (avoid printing binary data)
 */
void print_payload(const u_char *payload, u_int len, int indent)
{
    const u_int line_width = 16;    /* number of bytes per line */
    u_int len_rem = len;
    u_int offset = 0;

    while (len_rem > line_width) {
        int line_len = line_width % len_rem;
        fprintf(stderr, "%*s", indent, "");
        print_hex_ascii_line(payload + offset, line_len, offset);
        len_rem -= line_len;
        offset += line_width;
    }

    /* Might have left a partial line left. */
    if (len_rem > 0) {
        fprintf(stderr, "%*s", indent, "");
        print_hex_ascii_line(payload + offset, len_rem, offset);
    }

    return;
}



void tokenize(const string& str,
              vector<string>& tokens,
              const string& delimiters)
{
    // Skip delimiters at beginning.
    string::size_type lastPos = str.find_first_not_of(delimiters, 0);
    // Find first "non-delimiter".
    string::size_type pos     = str.find_first_of(delimiters, lastPos);

    while (string::npos != pos || string::npos != lastPos)
	{
	    // Found a token, add it to the vector.
	    tokens.push_back(str.substr(lastPos, pos - lastPos));
	    // Skip delimiters.  Note the "not_of"
	    lastPos = str.find_first_not_of(delimiters, pos);
	    // Find next "non-delimiter"
	    pos = str.find_first_of(delimiters, lastPos);
	}
}

void int_to_byte(const uint32_t i, char* p_byte_value)
{
    uint32_t int_value = i;
    for (int x = 0; x < 4; x++) {
        p_byte_value[x] = int_value & 0x000000FF;
        int_value = int_value >> 8;
        cout << "b[" << x << "] = " << hex << uppercase << setw(2) << setfill('0') <<(int)p_byte_value[x] << endl;
    }
}


void int_to_bit (const uint32_t i, int w )
{
    uint32_t z;
    string b="";
    for (z = 1 << (w-1); z > 0; z >>= 1)
    {
        b = (((i & z) == z) ? "1" : "0") + b;
    }
    cout << "bit[0.."<< w-1 << "]=" << b << endl;
} 

int fill_file_with_zeros(int fd, size_t nbytes)
{
    static const size_t BLOCKSIZE = 8192;
    char zeros[BLOCKSIZE];
    memset(zeros, 0, BLOCKSIZE);
    while (nbytes > 0) {
        size_t bytes_to_write = min(nbytes, BLOCKSIZE);
        ssize_t ret = write(fd, zeros, bytes_to_write);
        if (ret < 0) {
            perror("error in fill_file_with_zeros write");
            return -1;
        }
        nbytes -= bytes_to_write;
    }
    return 0;
}

string getID(string ip, const int32_t port)
{
    char sport[7];
    sprintf(sport, "%d", port);
    return ip.append(":").append(sport, strlen(sport));
}

string getIP(string id)
{
    return id.substr(0, id.find(":"));
}

int getPort(string id)
{
    string p = id.substr(id.find(":")+1, id.size());
    return strtol(p.c_str(), NULL, 10);
}

