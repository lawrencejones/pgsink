import { ResponseContext, RequestContext, HttpFile } from '../http/http';
import * as models from '../models/all';
import { Configuration} from '../configuration'

import { CheckResponseBody } from '../models/CheckResponseBody';
import { Import } from '../models/Import';
import { Table } from '../models/Table';
import { ObservableHealthApi } from './ObservableAPI';


import { HealthApiRequestFactory, HealthApiResponseProcessor} from "../apis/HealthApi";
export class PromiseHealthApi {
    private api: ObservableHealthApi

    public constructor(
        configuration: Configuration,
        requestFactory?: HealthApiRequestFactory,
        responseProcessor?: HealthApiResponseProcessor
    ) {
        this.api = new ObservableHealthApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * Health check for probes
     * Check Health
     */
    public healthCheck(options?: Configuration): Promise<CheckResponseBody> {
    	const result = this.api.healthCheck(options);
        return result.toPromise();
    }
	

}



import { ObservableImportsApi } from './ObservableAPI';


import { ImportsApiRequestFactory, ImportsApiResponseProcessor} from "../apis/ImportsApi";
export class PromiseImportsApi {
    private api: ObservableImportsApi

    public constructor(
        configuration: Configuration,
        requestFactory?: ImportsApiRequestFactory,
        responseProcessor?: ImportsApiResponseProcessor
    ) {
        this.api = new ObservableImportsApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * List all imports
     * List Imports
     */
    public importsList(options?: Configuration): Promise<Array<Import>> {
    	const result = this.api.importsList(options);
        return result.toPromise();
    }
	

}



import { ObservableTablesApi } from './ObservableAPI';


import { TablesApiRequestFactory, TablesApiResponseProcessor} from "../apis/TablesApi";
export class PromiseTablesApi {
    private api: ObservableTablesApi

    public constructor(
        configuration: Configuration,
        requestFactory?: TablesApiRequestFactory,
        responseProcessor?: TablesApiResponseProcessor
    ) {
        this.api = new ObservableTablesApi(configuration, requestFactory, responseProcessor);
    }

    /**
     * List all tables
     * List Tables
     * @param schema Comma separated list of Postgres schemas to filter by
     */
    public tablesList(schema?: string, options?: Configuration): Promise<Array<Table>> {
    	const result = this.api.tablesList(schema, options);
        return result.toPromise();
    }
	

}



