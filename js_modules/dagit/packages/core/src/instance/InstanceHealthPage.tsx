import {gql, useQuery} from '@apollo/client';
import * as React from 'react';

import {POLL_INTERVAL} from '../runs/useCursorPaginatedQuery';
import {Box} from '../ui/Box';
import {ColorsWIP} from '../ui/Colors';
import {PageHeader} from '../ui/PageHeader';
import {Heading, Subheading} from '../ui/Text';

import {DaemonList} from './DaemonList';
import {INSTANCE_HEALTH_FRAGMENT} from './InstanceHealthFragment';
import {InstanceTabs} from './InstanceTabs';
import {InstanceHealthQuery} from './types/InstanceHealthQuery';

export const InstanceHealthPage = () => {
  const queryData = useQuery<InstanceHealthQuery>(INSTANCE_HEALTH_QUERY, {
    fetchPolicy: 'cache-and-network',
    pollInterval: POLL_INTERVAL,
    notifyOnNetworkStatusChange: true,
  });

  const {loading, data} = queryData;

  const daemonContent = () => {
    if (loading && !data?.instance) {
      return <div style={{color: ColorsWIP.Gray400}}>Loading…</div>;
    }
    return data?.instance ? <DaemonList daemonHealth={data.instance.daemonHealth} /> : null;
  };

  return (
    <>
      <PageHeader
        title={<Heading>Instance status</Heading>}
        tabs={<InstanceTabs tab="health" queryData={queryData} />}
      />
      <Box padding={{vertical: 16, horizontal: 24}}>
        <Subheading>Daemon statuses</Subheading>
      </Box>
      {daemonContent()}
    </>
  );
};

export const INSTANCE_HEALTH_QUERY = gql`
  query InstanceHealthQuery {
    instance {
      ...InstanceHealthFragment
    }
  }

  ${INSTANCE_HEALTH_FRAGMENT}
`;
