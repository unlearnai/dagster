import {
  Box,
  Button,
  ButtonGroup,
  Checkbox,
  Group,
  IconName,
  Icon,
  MenuItem,
  Select,
  Spinner,
  Tab,
  Tabs,
  Tag,
  IconWrapper,
  Colors,
  Tooltip,
} from '@dagster-io/ui';
import * as React from 'react';
import styled from 'styled-components/macro';

import {useCopyToClipboard} from '../app/browser';
import {OptionsContainer, OptionsDivider} from '../gantt/VizComponents';

import {ExecutionStateDot} from './ExecutionStateDot';
import {LogLevel} from './LogLevel';
import {LogsFilterInput} from './LogsFilterInput';
import {LogFilter, LogFilterValue} from './LogsProvider';
import {
  extractLogCaptureStepsFromLegacySteps,
  ILogCaptureInfo,
  IRunMetadataDict,
  IStepState,
} from './RunMetadataProvider';
import {getRunFilterProviders} from './getRunFilterProviders';

export enum LogType {
  structured = 'structured',
  stdout = 'stdout',
  stderr = 'stderr',
}

interface ILogsToolbarProps {
  steps: string[];
  metadata: IRunMetadataDict;

  filter: LogFilter;
  onSetFilter: (filter: LogFilter) => void;
  logType: LogType;
  onSetLogType: (logType: LogType) => void;
  computeLogKey?: string;
  onSetComputeLogKey: (key: string) => void;
  computeLogUrl: string | null;
}

const logQueryToString = (logQuery: LogFilterValue[]) =>
  logQuery.map(({token, value}) => (token ? `${token}:${value}` : value)).join(' ');

export const LogsToolbar: React.FC<ILogsToolbarProps> = (props) => {
  const {
    steps,
    metadata,
    filter,
    onSetFilter,
    logType,
    onSetLogType,
    computeLogKey,
    onSetComputeLogKey,
    computeLogUrl,
  } = props;

  const activeItems = React.useMemo(
    () => new Set([logType === LogType.structured ? logType : LogType.stdout]),
    [logType],
  );

  return (
    <OptionsContainer>
      <ButtonGroup
        activeItems={activeItems}
        buttons={[
          {id: LogType.structured, icon: 'list', tooltip: 'Structured event logs'},
          {id: LogType.stdout, icon: 'wysiwyg', tooltip: 'Raw compute logs'},
        ]}
        onClick={(id) => onSetLogType(id)}
      />
      <OptionsDivider />
      {logType === 'structured' ? (
        <StructuredLogToolbar filter={filter} onSetFilter={onSetFilter} steps={steps} />
      ) : (
        <ComputeLogToolbar
          steps={steps}
          metadata={metadata}
          logType={logType}
          onSetLogType={onSetLogType}
          computeLogKey={computeLogKey}
          onSetComputeLogKey={onSetComputeLogKey}
          computeLogUrl={computeLogUrl}
        />
      )}
    </OptionsContainer>
  );
};

const resolveState = (metadata: IRunMetadataDict, logCapture: ILogCaptureInfo) => {
  // resolves the state of potentially many steps into a single state so that we can show the
  // execution dot representing the status of this log capture group (potentially at the process
  // level)
  if (logCapture.stepKeys.some((stepKey) => metadata.steps[stepKey].state === IStepState.RUNNING)) {
    return IStepState.RUNNING;
  }
  if (logCapture.stepKeys.some((stepKey) => metadata.steps[stepKey].state === IStepState.SKIPPED)) {
    return IStepState.SKIPPED;
  }
  if (
    logCapture.stepKeys.every((stepKey) => metadata.steps[stepKey].state === IStepState.SUCCEEDED)
  ) {
    return IStepState.SUCCEEDED;
  }
  return IStepState.FAILED;
};

const ComputeLogToolbar = ({
  steps,
  metadata,
  computeLogKey,
  onSetComputeLogKey,
  logType,
  onSetLogType,
  computeLogUrl,
}: {
  steps: string[];
  metadata: IRunMetadataDict;
  computeLogKey?: string;
  onSetComputeLogKey: (step: string) => void;
  logType: LogType;
  onSetLogType: (type: LogType) => void;
  computeLogUrl: string | null;
}) => {
  const logCaptureSteps =
    metadata.logCaptureSteps || extractLogCaptureStepsFromLegacySteps(Object.keys(metadata.steps));
  const isValidStepSelection = computeLogKey && logCaptureSteps[computeLogKey];
  const logKeyText = (logKey?: string) => {
    if (!logKey || !logCaptureSteps[logKey]) {
      return null;
    }
    const captureInfo = logCaptureSteps[logKey];
    if (captureInfo.stepKeys.length === 1 && logKey === captureInfo.stepKeys[0]) {
      return logKey;
    }
    if (captureInfo.pid) {
      return `pid: ${captureInfo.pid} (${captureInfo.stepKeys.length} steps)`;
    }
    return `${logKey} (${captureInfo.stepKeys.length} steps)`;
  };

  return (
    <Box
      flex={{justifyContent: 'space-between', alignItems: 'center', direction: 'row'}}
      style={{flex: 1}}
    >
      <Group direction="row" spacing={24} alignItems="center">
        <Select
          disabled={!steps.length}
          items={Object.keys(logCaptureSteps)}
          itemRenderer={(item: string, options: {handleClick: any; modifiers: any}) => (
            <MenuItem
              key={item}
              onClick={options.handleClick}
              text={logKeyText(item)}
              active={options.modifiers.active}
            />
          )}
          activeItem={computeLogKey}
          filterable={false}
          onItemSelect={(logKey) => {
            onSetComputeLogKey(logKey);
          }}
        >
          <Button disabled={!steps.length} rightIcon={<Icon name="expand_more" />}>
            {logKeyText(computeLogKey) || 'Select a step...'}
          </Button>
        </Select>
        {isValidStepSelection ? (
          <Tabs selectedTabId={logType} onChange={onSetLogType} size="small">
            <Tab id={LogType.stdout} title="stdout" />
            <Tab id={LogType.stderr} title="stderr" />
          </Tabs>
        ) : null}
      </Group>
      {isValidStepSelection ? (
        <Box flex={{direction: 'row', alignItems: 'center', gap: 12}}>
          {computeLogKey && logCaptureSteps[computeLogKey] ? (
            resolveState(metadata, logCaptureSteps[computeLogKey]) === IStepState.RUNNING ? (
              <Spinner purpose="body-text" />
            ) : (
              <ExecutionStateDot state={resolveState(metadata, logCaptureSteps[computeLogKey])} />
            )
          ) : null}
          {computeLogUrl ? (
            <Tooltip
              placement="top-end"
              content={
                computeLogKey && logCaptureSteps[computeLogKey]?.stepKeys.length === 1
                  ? `Download ${logCaptureSteps[computeLogKey]?.stepKeys[0]} compute logs`
                  : `Download compute logs`
              }
            >
              <DownloadLink aria-label="Download link" href={computeLogUrl} download>
                <Icon name="download_for_offline" color={Colors.Gray600} />
              </DownloadLink>
            </Tooltip>
          ) : null}
        </Box>
      ) : null}
    </Box>
  );
};

const DownloadLink = styled.a`
  border-radius: 4px;
  display: block;
  text-decoration: none;

  ${IconWrapper} {
    transition: background-color 100ms linear;
  }

  :hover ${IconWrapper} {
    background-color: ${Colors.Gray800};
  }

  :active ${IconWrapper}, :focus ${IconWrapper} {
    background-color: ${Colors.Dark};
  }

  :focus {
    outline: none;
  }
`;

const StructuredLogToolbar = ({
  filter,
  onSetFilter,
  steps,
}: {
  filter: LogFilter;
  onSetFilter: (filter: LogFilter) => void;
  steps: string[];
}) => {
  const [copyIcon, setCopyIcon] = React.useState<IconName>('assignment');
  const logQueryString = logQueryToString(filter.logQuery);
  const [queryString, setQueryString] = React.useState<string>(() => logQueryString);
  const copyToClipboard = useCopyToClipboard();

  const selectedStep = filter.logQuery.find((v) => v.token === 'step')?.value || null;
  const filterText = filter.logQuery.reduce((accum, value) => accum + value.value, '');

  // Reset the query string if the filter is updated, allowing external behavior
  // (e.g. clicking a Gantt step) to set the input.
  React.useEffect(() => {
    setQueryString(logQueryString);
  }, [logQueryString]);

  const onChange = (value: string) => {
    const tokens = value.split(/\s+/);
    const logQuery = tokens.map((item) => {
      const segments = item.split(':');
      if (segments.length > 1) {
        return {token: segments[0], value: segments[1]};
      }
      return {value: segments[0]};
    });
    onSetFilter({...filter, logQuery: logQuery as LogFilterValue[]});
    setQueryString(value);
  };

  // Restore the clipboard icon after a delay.
  React.useEffect(() => {
    let token: any;
    if (copyIcon === 'assignment_turned_in') {
      token = setTimeout(() => {
        setCopyIcon('assignment');
      }, 2000);
    }
    return () => {
      token && clearTimeout(token);
    };
  }, [copyIcon]);

  return (
    <>
      <LogsFilterInput
        value={queryString}
        suggestionProviders={getRunFilterProviders(steps)}
        onChange={onChange}
      />
      {filterText ? (
        <NonMatchCheckbox
          checked={filter.hideNonMatches}
          onChange={(event: React.ChangeEvent<HTMLInputElement>) =>
            onSetFilter({...filter, hideNonMatches: event.currentTarget.checked})
          }
          label="Hide non-matches"
        />
      ) : null}
      <OptionsDivider />
      <Group direction="row" spacing={4} alignItems="center">
        {Object.keys(LogLevel).map((level) => {
          const enabled = filter.levels[level];
          return (
            <FilterButton
              key={level}
              onClick={() =>
                onSetFilter({
                  ...filter,
                  levels: {
                    ...filter.levels,
                    [level]: !enabled,
                  },
                })
              }
            >
              <Tag
                key={level}
                intent={enabled ? 'primary' : 'none'}
                interactive
                minimal={!enabled}
                round
              >
                {level.toLowerCase()}
              </Tag>
            </FilterButton>
          );
        })}
      </Group>
      {selectedStep && <OptionsDivider />}
      <div style={{minWidth: 15, flex: 1}} />
      <Button
        icon={<Icon name={copyIcon} />}
        onClick={() => {
          copyToClipboard(window.location.href);
          setCopyIcon('assignment_turned_in');
        }}
      >
        Copy URL
      </Button>
    </>
  );
};

const NonMatchCheckbox = styled(Checkbox)`
  &&& {
    margin: 0 4px 0 12px;
  }

  white-space: nowrap;
`;

const FilterButton = styled.button`
  background: none;
  border: none;
  padding: 0;
  margin: 0;
  cursor: pointer;
  display: block;

  :focus {
    outline: none;
  }
`;
