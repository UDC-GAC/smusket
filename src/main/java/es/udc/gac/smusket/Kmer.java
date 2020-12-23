/*
 * Copyright (C) 2021 Universidade da Coruña
 *
 * This file is part of SparkMusket.
 *
 * SparkMusket is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * SparkMusket is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with SparkMusket. If not, see <http://www.gnu.org/licenses/>.
 */
package es.udc.gac.smusket;

import java.nio.charset.StandardCharsets;

import es.udc.gac.smusket.util.Constants;
import es.udc.gac.smusket.util.MurmurHash3;

public class Kmer {

	public static Constants Constants;

	private static final byte NBASE = 0;

	private static final byte[] ENCODE = { NBASE, NBASE, NBASE, NBASE, NBASE, NBASE,
			NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE,
			NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE,
			NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE,
			NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE,
			NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE,
			NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, 0, NBASE,
			1, NBASE, NBASE, NBASE, 2, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE,
			NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, 3, NBASE, NBASE, NBASE, NBASE,
			NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, 0, NBASE, 1,
			NBASE, NBASE, NBASE, 2, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE, NBASE,
			NBASE, NBASE, NBASE, NBASE, NBASE, 3, NBASE, NBASE, NBASE, NBASE, NBASE,
			NBASE, NBASE, NBASE, NBASE, NBASE, NBASE };

	private static final short[] BASE_SWAP = { 0x00, 0x40, 0x80, 0xc0, 0x10, 0x50,
			0x90, 0xd0, 0x20, 0x60, 0xa0, 0xe0, 0x30, 0x70, 0xb0, 0xf0, 0x04, 0x44,
			0x84, 0xc4, 0x14, 0x54, 0x94, 0xd4, 0x24, 0x64, 0xa4, 0xe4, 0x34, 0x74,
			0xb4, 0xf4, 0x08, 0x48, 0x88, 0xc8, 0x18, 0x58, 0x98, 0xd8, 0x28, 0x68,
			0xa8, 0xe8, 0x38, 0x78, 0xb8, 0xf8, 0x0c, 0x4c, 0x8c, 0xcc, 0x1c, 0x5c,
			0x9c, 0xdc, 0x2c, 0x6c, 0xac, 0xec, 0x3c, 0x7c, 0xbc, 0xfc, 0x01, 0x41,
			0x81, 0xc1, 0x11, 0x51, 0x91, 0xd1, 0x21, 0x61, 0xa1, 0xe1, 0x31, 0x71,
			0xb1, 0xf1, 0x05, 0x45, 0x85, 0xc5, 0x15, 0x55, 0x95, 0xd5, 0x25, 0x65,
			0xa5, 0xe5, 0x35, 0x75, 0xb5, 0xf5, 0x09, 0x49, 0x89, 0xc9, 0x19, 0x59,
			0x99, 0xd9, 0x29, 0x69, 0xa9, 0xe9, 0x39, 0x79, 0xb9, 0xf9, 0x0d, 0x4d,
			0x8d, 0xcd, 0x1d, 0x5d, 0x9d, 0xdd, 0x2d, 0x6d, 0xad, 0xed, 0x3d, 0x7d,
			0xbd, 0xfd, 0x02, 0x42, 0x82, 0xc2, 0x12, 0x52, 0x92, 0xd2, 0x22, 0x62,
			0xa2, 0xe2, 0x32, 0x72, 0xb2, 0xf2, 0x06, 0x46, 0x86, 0xc6, 0x16, 0x56,
			0x96, 0xd6, 0x26, 0x66, 0xa6, 0xe6, 0x36, 0x76, 0xb6, 0xf6, 0x0a, 0x4a,
			0x8a, 0xca, 0x1a, 0x5a, 0x9a, 0xda, 0x2a, 0x6a, 0xaa, 0xea, 0x3a, 0x7a,
			0xba, 0xfa, 0x0e, 0x4e, 0x8e, 0xce, 0x1e, 0x5e, 0x9e, 0xde, 0x2e, 0x6e,
			0xae, 0xee, 0x3e, 0x7e, 0xbe, 0xfe, 0x03, 0x43, 0x83, 0xc3, 0x13, 0x53,
			0x93, 0xd3, 0x23, 0x63, 0xa3, 0xe3, 0x33, 0x73, 0xb3, 0xf3, 0x07, 0x47,
			0x87, 0xc7, 0x17, 0x57, 0x97, 0xd7, 0x27, 0x67, 0xa7, 0xe7, 0x37, 0x77,
			0xb7, 0xf7, 0x0b, 0x4b, 0x8b, 0xcb, 0x1b, 0x5b, 0x9b, 0xdb, 0x2b, 0x6b,
			0xab, 0xeb, 0x3b, 0x7b, 0xbb, 0xfb, 0x0f, 0x4f, 0x8f, 0xcf, 0x1f, 0x5f,
			0x9f, 0xdf, 0x2f, 0x6f, 0xaf, 0xef, 0x3f, 0x7f, 0xbf, 0xff };

	private byte[] encodedBases;

	public Kmer() {
		encodedBases = new byte[Constants.MAX_K_DIV_4];
	}

	public Kmer(Kmer km) {
		encodedBases = new byte[km.encodedBases.length];
		System.arraycopy(km.encodedBases, 0, encodedBases, 0, encodedBases.length);
	}

	public Kmer(Sequence read) {
		encodedBases = new byte[Constants.MAX_K_DIV_4];
		Kmer.encodeBases(encodedBases, read.getBases(), 0);
	}

	public void setBases(byte[] bases) {
		setBases(bases, 0);
	}

	public void setBases(byte[] bases, int pos) {
		Kmer.memzero(encodedBases);
		Kmer.encodeBases(encodedBases, bases, pos);
	}

	public void setBases(Kmer km) {
		System.arraycopy(km.encodedBases, 0, encodedBases, 0, encodedBases.length);
	}

	public void setBase(byte base, int index) {
		int j = (index & 3) * 2;
		int l = index >> 2;

		encodedBases[l] &= ~(0x03 << j);
		encodedBases[l] |= (base << j);
	}

	public byte getBase(int index) {
		return (byte) (((encodedBases[index >> 2]) >> ((index & 3) * 2)) & 0x03);
	}

	public void forwardBase(byte b) {

		shiftRight(2);
		encodedBases[Constants.K_BYTES_MINUS_1] &= Constants.K_MODMASK;

		switch (ENCODE[b]) {
		case 0:
			encodedBases[Constants.K_BYTES_MINUS_1] |= 0x00 << Constants.SHIFT;
			break;
		case 1:
			encodedBases[Constants.K_BYTES_MINUS_1] |= 0x01 << Constants.SHIFT;
			break;
		case 2:
			encodedBases[Constants.K_BYTES_MINUS_1] |= 0x02 << Constants.SHIFT;
			break;
		case 3:
			encodedBases[Constants.K_BYTES_MINUS_1] |= 0x03 << Constants.SHIFT;
			break;
		}
	}

	public void backwardBase(byte b) {

		shiftLeft(2);
		encodedBases[Constants.K_BYTES_MINUS_1] &= Constants.K_MODMASK;

		if (Constants.K_SIZE % 4 == 0 && Constants.K_BYTES < Constants.MAX_K_DIV_4)
			encodedBases[Constants.K_BYTES] = 0x00;

		switch (ENCODE[b]) {
		case 0:
			encodedBases[0] |= 0x00;
			break;
		case 1:
			encodedBases[0] |= 0x01;
			break;
		case 2:
			encodedBases[0] |= 0x02;
			break;
		case 3:
			encodedBases[0] |= 0x03;
			break;
		}
	}

	public void twin(Kmer km) {
		int tmp, i;
		setBases(km);

		for (i = 0; i < Constants.K_BYTES; i++)
			encodedBases[i] = (byte) ~km.encodedBases[i]; // ~ Unary bitwise complement

		encodedBases[Constants.K_BYTES_MINUS_1] ^= ~Constants.K_MODMASK; // ^ bitwise XOR

		// shift to byte alignment
		shiftLeft(Constants.SHIFT_LEFT);

		for (i = 0; i < Constants.K_BYTES_DIV_2; i++) {
			tmp = 0xFF & encodedBases[i];
			encodedBases[i] = (byte) BASE_SWAP[0xFF & encodedBases[Constants.K_BYTES_MINUS_1 - i]];
			encodedBases[Constants.K_BYTES_MINUS_1 - i] = (byte) BASE_SWAP[tmp];
		}

		if ((Constants.K_BYTES & 1) == 1)
			encodedBases[Constants.K_BYTES_DIV_2] = (byte) BASE_SWAP[(0xFF & encodedBases[Constants.K_BYTES_DIV_2])];
	}

	private void shiftLeft(int shift) {
		for (int i = Constants.K_BYTES - 1; i > 0; i--) {
			encodedBases[i] <<= shift;
			encodedBases[i] |= (0xFF & encodedBases[i - 1]) >> (8 - shift);
		}
		encodedBases[0] <<= shift;
	}

	private void shiftRight(int shift) {
		for (int i = 0; i < Constants.K_BYTES - 1; i++) {
			encodedBases[i] = (byte) ((0xFF & encodedBases[i]) >> shift);
			encodedBases[i] |= encodedBases[i + 1] << (8 - shift);
		}
		encodedBases[Constants.K_BYTES - 1] = (byte) ((0xFF & encodedBases[Constants.K_BYTES_MINUS_1]) >> shift);
	}

	public boolean lower(Kmer other) {
		return Kmer.memcmp(this.encodedBases, other.encodedBases, encodedBases.length) < 0;
	}

	@Override
	public boolean equals(Object other) {
		if (other == null) return false;
		if (other == this) return true;
		if (!(other instanceof Kmer)) return false;
		return Kmer.memcmp(this.encodedBases, ((Kmer)other).encodedBases, encodedBases.length) == 0;
	}

	@Override
	public int hashCode() {
		return Long.hashCode(hash());
	}

	@Override
	public String toString() {
		byte[] bases = new byte[Constants.K_SIZE];
		Kmer.decodeBases(encodedBases, bases);
		return new String(bases, StandardCharsets.UTF_8);
	}

	public long hash() {
		return MurmurHash3.murmurhash3_x64_128(encodedBases, 0, Constants.K_BYTES, 0);
	}

	private static final int memcmp(final byte[] a, final byte[] b, int length) {
		/*
		 * This function is specific for our use case and it is not
		 * intended for general usage  
		 */
		for (int i = 0; i < length; i++) {
			if (a[i] != b[i])
				return (a[i] & 0xFF) - (b[i] & 0xFF);
		}
		return a.length - b.length;
	}

	private static final void encodeBases(byte[] encodedBases, byte[] bases, int offset) {
		int i, j, l;

		for (i = 0; i < Constants.K_SIZE; i++) {
			j = (i & 3) * 2;
			l = i >> 2;

		switch (ENCODE[bases[i + offset]]) {
		case 0:
			break;
		case 1:
			encodedBases[l] |= (0x01 << j);
			break;
		case 2: 
			encodedBases[l] |= (0x02 << j);
			break;
		case 3:
			encodedBases[l] |= (0x03 << j);
			break;
		}
		}
	}

	private static final void decodeBases(byte[] encodedBases, byte[] bases) {
		int i, j, l, s = 0;

		for (i = 0; i < Constants.K_SIZE; i++) {
			j = (i & 3) * 2;
			l = i >> 2;

		switch (((encodedBases[l]) >> j) & 0x03) {
		case 0x00:
			bases[s] = 'A';
			s++;
			break;
		case 0x01:
			bases[s] = 'C';
			s++;
			break;
		case 0x02:
			bases[s] = 'G';
			s++;
			break;
		case 0x03:
			bases[s] = 'T';
			s++;
			break;
		}
		}
	}

	private static final void memzero(byte[] array) {
		array[0] = NBASE;
		for (int i = 1; i < array.length; i += i)
			System.arraycopy(array, NBASE, array, i, ((array.length - i) < i) ? (array.length - i) : i);
	}
}