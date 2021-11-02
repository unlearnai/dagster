import {useLocation} from 'react-router-dom';

const TELEMETRY_BROWSER_ID = 'TELEMETRY_BROWSER_ID';

const getTelemetryBrowserId = () => {
  let telemetryId = localStorage.getItem(TELEMETRY_BROWSER_ID);
  if (!telemetryId) {
    //randomluy generate id
    telemetryId =
      Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
    localStorage.setItem(TELEMETRY_BROWSER_ID, telemetryId);
  }
  return telemetryId;
};

export const Telemetry = () => {
  const location = useLocation();
  const telemetryId = getTelemetryBrowserId();
  console.log('I AM AT:', {
    path: location.pathname,
    telemetryId,
  });
  return null;
};
