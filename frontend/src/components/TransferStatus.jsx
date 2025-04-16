import React, { useState } from 'react';
import {
  Box,
  Paper,
  Typography,
  Button,
  Alert,
  CircularProgress,
  LinearProgress
} from '@mui/material';
import axios from 'axios';

const TransferStatus = ({ config, onBack }) => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(''));
  const [success, setSuccess] = useState('');
  const [recordCount, setRecordCount] = useState(0);

  const handleTransfer = async () => {
    setLoading(true);
    setError('');
    setSuccess('');
    try {
      const response = await axios.post('http://localhost:8080/api/ingestion/transfer', config);
      setSuccess(response.data.message);
      setRecordCount(response.data.recordCount);
    } catch (err) {
      setError(err.response?.data?.error || 'Transfer failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Box sx={{ py: 3 }}>
      <Typography variant="h5" gutterBottom>
        Transfer Status
      </Typography>

      <Paper sx={{ p: 3, mt: 2 }}>
        {!loading && !success && !error && (
          <Box sx={{ textAlign: 'center' }}>
            <Typography variant="body1" gutterBottom>
              Ready to transfer data from {config.sourceType === 'CLICKHOUSE' ? 'ClickHouse' : 'Flat File'}
            </Typography>
            <Button
              variant="contained"
              onClick={handleTransfer}
              sx={{ mt: 2 }}
            >
              Start Transfer
            </Button>
          </Box>
        )}

        {loading && (
          <Box sx={{ textAlign: 'center' }}>
            <Typography variant="body1" gutterBottom>
              Transferring data...
            </Typography>
            <LinearProgress sx={{ mt: 2 }} />
          </Box>
        )}

        {error && (
          <Alert severity="error" sx={{ mt: 2 }}>
            {error}
          </Alert>
        )}

        {success && (
          <Box>
            <Alert severity="success" sx={{ mb: 2 }}>
              {success}
            </Alert>
            <Typography variant="body1">
              Total records processed: {recordCount}
            </Typography>
          </Box>
        )}
      </Paper>

      <Box sx={{ mt: 3 }}>
        <Button onClick={onBack}>
          Back
        </Button>
      </Box>
    </Box>
  );
};

export default TransferStatus;