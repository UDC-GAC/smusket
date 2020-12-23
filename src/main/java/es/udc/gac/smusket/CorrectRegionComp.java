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

import java.util.Comparator;

public abstract class CorrectRegionComp implements Comparator<CorrectRegion> {

	public static class CorrectRegionPingComp extends CorrectRegionComp {
		@Override
		public int compare(CorrectRegion a, CorrectRegion b) {
			if (a.length() - b.length() <= 0)
				return 1;
			return -1;
		}
	}

	public static class CorrectRegionPongComp extends CorrectRegionComp {
		@Override
		public int compare(CorrectRegion a, CorrectRegion b) {
			if (a.length() - b.length() >= 0)
				return 1;
			return -1;
		}
	}
}
