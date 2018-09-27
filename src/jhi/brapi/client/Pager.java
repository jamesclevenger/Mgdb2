/*******************************************************************************
 * MGDB - Mongo Genotype DataBase
 * Copyright (C) 2016, 2018, <CIRAD> <IRD>
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License, version 3 as published by
 * the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * See <http://www.gnu.org/licenses/agpl.html> for details about GNU General
 * Public License V3.
 *******************************************************************************/
package jhi.brapi.client;

import jhi.brapi.api.Metadata;
import jhi.brapi.api.Pagination;

public class Pager {
    private boolean isPaging = true;
    private int pageSize = 1000;
    private int page = 0;

    public Pager() {
    }

    public Pager(int pageSize) {
        this.pageSize = pageSize;
    }

    public void paginate(Metadata metadata) {
        Pagination p = metadata.getPagination();
        if (p.getTotalPages() == 0 || p.getCurrentPage() == p.getTotalPages() - 1) {
            this.isPaging = false;
        } else {
            this.pageSize = p.getPageSize();
            this.page = p.getCurrentPage() + 1;
        }
    }

    public boolean isPaging() {
        return this.isPaging;
    }

    public void setPaging(boolean paging) {
        this.isPaging = paging;
    }

    public int getPageSize() {
        return this.pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getPage() {
        return this.page;
    }

    public void setPage(int page) {
        this.page = page;
    }
}

