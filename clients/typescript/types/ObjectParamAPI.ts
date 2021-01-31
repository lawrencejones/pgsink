import { ResponseContext, RequestContext, HttpFile } from '../http/http';
import * as models from '../models/all';
import { Configuration} from '../configuration'

import { CheckResponseBody } from '../models/CheckResponseBody';
import { Import } from '../models/Import';
import { Table } from '../models/Table';

import { ObservableHealthApi } from "./ObservableAPI";
import { HealthApiRequestFactory, HealthApiResponseProcessor} from "../apis/HealthApi";

export interface HealthApiHealthCheckRequest {
}


export class ObjectHealthApi {
    private api: ObservableHealthApi

    public constructor(configuration: Configuration, requestFactory?: HealthApiRequestFactory, responseProcessor?: HealthApiResponseProcessor) {
        this.api = new ObservableHealthApi(configuration, requestFactory, responseProcessor);
	}

    /**
     * Health check for probes
     * Check Health
     * @param param the request object
     */
    public healthCheck(param: HealthApiHealthCheckRequest, options?: Configuration): Promise<CheckResponseBody> {
        return this.api.healthCheck( options).toPromise();
    }
	

}




import { ObservableImportsApi } from "./ObservableAPI";
import { ImportsApiRequestFactory, ImportsApiResponseProcessor} from "../apis/ImportsApi";

export interface ImportsApiImportsListRequest {
}


export class ObjectImportsApi {
    private api: ObservableImportsApi

    public constructor(configuration: Configuration, requestFactory?: ImportsApiRequestFactory, responseProcessor?: ImportsApiResponseProcessor) {
        this.api = new ObservableImportsApi(configuration, requestFactory, responseProcessor);
	}

    /**
     * List all imports
     * List Imports
     * @param param the request object
     */
    public importsList(param: ImportsApiImportsListRequest, options?: Configuration): Promise<Array<Import>> {
        return this.api.importsList( options).toPromise();
    }
	

}




import { ObservableTablesApi } from "./ObservableAPI";
import { TablesApiRequestFactory, TablesApiResponseProcessor} from "../apis/TablesApi";

export interface TablesApiTablesListRequest {
    /**
     * Comma separated list of Postgres schemas to filter by
     * @type string
     * @memberof TablesApitablesList
     */
    schema?: string
}


export class ObjectTablesApi {
    private api: ObservableTablesApi

    public constructor(configuration: Configuration, requestFactory?: TablesApiRequestFactory, responseProcessor?: TablesApiResponseProcessor) {
        this.api = new ObservableTablesApi(configuration, requestFactory, responseProcessor);
	}

    /**
     * List all tables
     * List Tables
     * @param param the request object
     */
    public tablesList(param: TablesApiTablesListRequest, options?: Configuration): Promise<Array<Table>> {
        return this.api.tablesList(param.schema,  options).toPromise();
    }
	

}



