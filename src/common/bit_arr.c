/*
    4store - a clustered RDF storage and query engine

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.


    Copyright 2011 Manuel Salvadores
 */

#include <stdlib.h>
#include <string.h>

char fs_bit_mask_set(short nbit,short value) {
    
    if (value)
        return  0xFF;

    if (nbit == 0) 
        return 0x7F;
    else if (nbit == 1)
        return 0xBF;
    else if (nbit == 2) 
        return 0xDF;
    else if (nbit == 3) 
        return 0xEF;
    else if (nbit == 4) 
        return 0xF7;
    else if (nbit == 5) 
        return 0xFB;
    else if (nbit == 6) 
        return 0xFD;
    else if (nbit == 7) 
        return 0xFE;
    return 0xFF;
}


char fs_bit_mask_get(short nbit) {
    if (nbit == 0) 
        return 0x80;
    else if (nbit == 1)
        return 0x40;
    else if (nbit == 2) 
        return 0x20;
    else if (nbit == 3) 
        return 0x10;
    else if (nbit == 4) 
        return 0x08;
    else if (nbit == 5) 
        return 0x04;
    else if (nbit == 6) 
        return 0x02;
    else if (nbit == 7) 
        return 0x01;
    return 0x00;
}

unsigned char *fs_new_bit_array(long n) {
    int extra = (n % 8) ? 1 : 0;
    size_t ms = ((n / 8)+extra) * sizeof(unsigned char);
    unsigned char *p = malloc(ms);
    memset(p,0xFF,ms);
    return p;
}

void fs_bit_array_set(unsigned char *p,long i,int value) {
    p[i/8] &= fs_bit_mask_set(i % 8,value);
}

short fs_bit_array_get(unsigned char *p,long i) {
    return p[i/8] & fs_bit_mask_get(i % 8) ? 1 : 0;
}

void fs_bit_array_destroy(unsigned char *p) {
    if (p)
        free(p);
}
