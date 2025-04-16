import React, { useState } from 'react';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { Container, CssBaseline, Box, Stepper, Step, StepLabel } from '@mui/material';
import SourceSelection from './components/SourceSelection';
import ConnectionConfig from './components/ConnectionConfig';
import SchemaSelection from './components/SchemaSelection';
import DataPreview from './components/DataPreview';
import TransferStatus from './components/TransferStatus';

const theme = createTheme({
  palette: {
    mode: 'light',
    primary: {
      main: '#1976d2',
    },
    secondary: {
      main: '#dc004e',
    },
  },
});

const steps = [
  'Select Source',
  'Configure Connection',
  'Select Schema',
  'Preview Data',
  'Transfer Status'
];

function App() {
  const [activeStep, setActiveStep] = useState(0);
  const [config, setConfig] = useState({
    sourceType: '',
    clickHouseConfig: {
      host: 'localhost',
      port: 8123,
      database: 'default',
      username: 'default',
      jwtToken: ''
    },
    flatFilePath: '',
    delimiter: ',',
    selectedTables: [],
    selectedColumns: [],
    joinCondition: '',
    joinKeys: [],
    previewMode: false,
    previewLimit: 100
  });

  const handleNext = () => {
    setActiveStep((prevStep) => prevStep + 1);
  };

  const handleBack = () => {
    setActiveStep((prevStep) => prevStep - 1);
  };

  const handleConfigUpdate = (updates) => {
    setConfig((prev) => ({ ...prev, ...updates }));
  };

  const getStepContent = (step) => {
    switch (step) {
      case 0:
        return (
          <SourceSelection
            config={config}
            onUpdate={handleConfigUpdate}
            onNext={handleNext}
          />
        );
      case 1:
        return (
          <ConnectionConfig
            config={config}
            onUpdate={handleConfigUpdate}
            onNext={handleNext}
            onBack={handleBack}
          />
        );
      case 2:
        return (
          <SchemaSelection
            config={config}
            onUpdate={handleConfigUpdate}
            onNext={handleNext}
            onBack={handleBack}
          />
        );
      case 3:
        return (
          <DataPreview
            config={config}
            onUpdate={handleConfigUpdate}
            onNext={handleNext}
            onBack={handleBack}
          />
        );
      case 4:
        return (
          <TransferStatus
            config={config}
            onBack={handleBack}
          />
        );
      default:
        return 'Unknown step';
    }
  };

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Container maxWidth="lg">
        <Box sx={{ width: '100%', mt: 4 }}>
          <Stepper activeStep={activeStep}>
            {steps.map((label) => (
              <Step key={label}>
                <StepLabel>{label}</StepLabel>
              </Step>
            ))}
          </Stepper>
          <Box sx={{ mt: 4 }}>
            {getStepContent(activeStep)}
          </Box>
        </Box>
      </Container>
    </ThemeProvider>
  );
}

export default App;