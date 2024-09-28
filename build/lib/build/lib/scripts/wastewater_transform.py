import os
import globus_sdk
from globus_sdk.scopes import AuthScopes
from globus_compute_sdk import Client
from globus_compute_sdk.sdk.login_manager import AuthorizerLoginManager
from globus_compute_sdk.sdk.login_manager.manager import ComputeScopeBuilder


def transform(*args, **kwargs):
    import pandas as pd
    import numpy as np

    # load input
    odata = pd.read_csv(kwargs["file"])

    # keep relevant info, rename
    odata = odata.loc[
        odata.method != 0, ["sars_cov_2", "sample_collect_date"]
    ].reset_index(drop=True)
    odata.columns = ["gene_copy", "date"]

    # convert date to numerical (equivalent to what R does)
    reference_date = pd.Timestamp("1970-01-01")
    odata["date"] = pd.to_datetime(odata["date"])
    odata["num_date"] = (odata["date"] - reference_date).dt.days

    # assign year
    odata["year"] = np.nan
    odata.loc[odata["num_date"] < 19358, "year"] = 2022
    odata.loc[(odata["num_date"] >= 19358) & (odata["num_date"] < 19724), "year"] = 2023
    odata.loc[odata["num_date"] >= 19724, "year"] = 2024

    # calculate yearday and time
    odata["yearday"] = odata["num_date"]
    odata.loc[odata["year"] == 2022, "yearday"] = (
        odata.loc[odata["year"] == 2022, "num_date"] - (52 * 365) - 12
    )
    odata.loc[odata["year"] == 2023, "yearday"] = (
        odata.loc[odata["year"] == 2023, "num_date"] - (53 * 365) - 12
    )
    odata.loc[odata["year"] == 2024, "yearday"] = (
        odata.loc[odata["year"] == 2024, "num_date"] - (54 * 365) - 12
    )

    odata["year_day"] = odata["num_date"] - (52 * 365) - 12
    odata['new_time'] = odata['year_day'] - (odata['year_day'].iloc[0]-1)

    # calculate values
    odata["sum_genes"] = odata["gene_copy"]
    odata["log_gene_copies"] = np.log10(odata["gene_copy"])

    odata["epi_week2"] = (odata["yearday"] - 1) / 7 + 1
    odata["epi_week"] = np.floor(odata["epi_week2"])

    return args, kwargs

    # # validate with R output
    # oldata = pd.read_csv('Obriendata.csv')

    # # compare, round for numerical comparison
    # df1 = odata.round(5)
    # df2 = oldata.round(5)

    # assert(df1.compare(df2).shape[0] == 0)


if __name__ == "__main__":
    c = globus_sdk.ConfidentialAppAuthClient(
        os.environ["GLOBUS_COMPUTE_CLIENT_ID"],
        os.environ["GLOBUS_COMPUTE_CLIENT_SECRET"],
    )

    gc_authorizer = globus_sdk.ClientCredentialsAuthorizer(c, Client.FUNCX_SCOPE)
    openid_authorizer = globus_sdk.ClientCredentialsAuthorizer(c, AuthScopes.openid)

    ComputeScopes = ComputeScopeBuilder()
    compute_login_manager = AuthorizerLoginManager(
        authorizers={
            ComputeScopes.resource_server: gc_authorizer,
            AuthScopes.resource_server: openid_authorizer,
        }
    )
    compute_login_manager.ensure_logged_in()

    gc = Client(login_manager=compute_login_manager)
    print(gc.register_function(transform))
    # transform(file='/Users/valeriehayot-sasson/aero/a497d439-844b-4229-a00d-15cfc7822592')
