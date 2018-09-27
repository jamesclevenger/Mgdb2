package jhi.brapi.client;

import java.util.List;
import jhi.brapi.api.BrapiBaseResource;
import jhi.brapi.api.BrapiListResource;
import jhi.brapi.api.allelematrices.BrapiAlleleMatrixDataset;
import jhi.brapi.api.authentication.BrapiSessionToken;
import jhi.brapi.api.authentication.BrapiTokenLoginPost;
import jhi.brapi.api.calls.BrapiCall;
import jhi.brapi.api.genomemaps.BrapiGenomeMap;
import jhi.brapi.api.genomemaps.BrapiMapMetaData;
import jhi.brapi.api.genomemaps.BrapiMarkerPosition;
import jhi.brapi.api.markerprofiles.BrapiAlleleMatrix;
import jhi.brapi.api.markerprofiles.BrapiAlleleMatrixSearchPost;
import jhi.brapi.api.markerprofiles.BrapiMarkerProfile;
import jhi.brapi.api.markerprofiles.BrapiMarkerProfilePost;
import jhi.brapi.api.studies.BrapiStudies;
import jhi.brapi.api.studies.BrapiStudiesPost;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.Field;
import retrofit2.http.FormUrlEncoded;
import retrofit2.http.GET;
import retrofit2.http.POST;
import retrofit2.http.Path;
import retrofit2.http.Query;

public interface RetrofitService {
    @GET(value="calls")
    public Call<BrapiListResource<BrapiCall>> getCalls(@Query(value="dataType") String var1, @Query(value="pageSize") Integer var2, @Query(value="page") Integer var3);

    @POST(value="token")
    public Call<BrapiSessionToken> getAuthToken(@Body BrapiTokenLoginPost var1);

    @GET(value="studies-search")
    public Call<BrapiListResource<BrapiStudies>> getStudies(@Query(value="studyType") String var1, @Query(value="pageSize") Integer var2, @Query(value="page") Integer var3);

    @GET(value="allelematrices")
    public Call<BrapiListResource<BrapiAlleleMatrixDataset>> getMatrices(@Query(value="studyDbId") String var1, @Query(value="pageSize") Integer var2, @Query(value="page") Integer var3);

    @POST(value="studies-search")
    public Call<BrapiListResource<BrapiStudies>> getStudiesPost(@Body BrapiStudiesPost var1);

    @GET(value="maps")
    public Call<BrapiListResource<BrapiGenomeMap>> getMaps(@Query(value="species") String var1, @Query(value="type") String var2, @Query(value="pageSize") Integer var3, @Query(value="page") Integer var4);

    @GET(value="maps/{id}")
    public Call<BrapiBaseResource<BrapiMapMetaData>> getMapMetaData(@Path(value="id") String var1);

    @GET(value="maps/{id}/positions")
    public Call<BrapiListResource<BrapiMarkerPosition>> getMapMarkerData(@Path(value="id") String var1, @Query(value="linkageGroupId") List<String> var2, @Query(value="pageSize") Integer var3, @Query(value="page") Integer var4);

    @GET(value="markerprofiles")
    public Call<BrapiListResource<BrapiMarkerProfile>> getMarkerProfiles(@Query(value="markerprofileDbId") String var1, @Query(value="studyDbId") String var2, @Query(value="sampleDbId") String var3, @Query(value="extractDbId") String var4, @Query(value="pageSize") Integer var5, @Query(value="page") Integer var6);

    @POST(value="makrerprofiles-search")
    public Call<BrapiListResource<BrapiMarkerProfile>> getMarkerProfiles(@Body BrapiMarkerProfilePost var1);

    @FormUrlEncoded
    @POST(value="allelematrix-search")
    public Call<BrapiBaseResource<BrapiAlleleMatrix>> getAlleleMatrix(@Field(value="markerprofileDbId") List<String> var1, @Field(value="markerDbId") List<String> var2, @Field(value="format") String var3, @Field(value="expandHomozygotes") Boolean var4, @Field(value="unknownString") String var5, @Field(value="sepPhased") String var6, @Field(value="sepUnphased") String var7, @Field(value="pageSize") Integer var8, @Field(value="page") Integer var9);

    @FormUrlEncoded
    @POST(value="allelematrix-search")
    public Call<BrapiBaseResource<BrapiAlleleMatrix>> getAlleleMatrix(@Field(value="matrixDbId") String var1, @Field(value="format") String var2, @Field(value="expandHomozygotes") Boolean var3, @Field(value="unknownString") String var4, @Field(value="sepPhased") String var5, @Field(value="sepUnphased") String var6, @Field(value="pageSize") Integer var7, @Field(value="page") Integer var8);

    @POST(value="allelematrix-search")
    public Call<BrapiBaseResource<BrapiAlleleMatrix>> getAlleleMatrix(@Body BrapiAlleleMatrixSearchPost var1);

    @GET(value="allelematrix-search/status/{id}")
    public Call<BrapiListResource<Object>> getAlleleMatrixStatus(@Path(value="id") String var1);
}

