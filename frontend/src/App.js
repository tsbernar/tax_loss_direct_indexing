import React from 'react';
import { template_dark } from './PlotlyTemplate';
import {
  ChakraProvider,
  useColorModeValue,
  Box,
  Text,
  Spinner,
  Grid,
  theme,
  Tabs,
  TabList,
  TabPanels,
  Tab,
  TabPanel,
} from '@chakra-ui/react';
import { ColorModeSwitcher } from './ColorModeSwitcher';
import Plot from 'react-plotly.js';

async function getAPIData(endpoint) {
  return fetch('api/' + endpoint, { credentials: 'include', method: 'GET' })
    .then(response => {
      if (!response.ok) {
        throw new Error(
          `Could not fetch data from api. Status: ${response.status}: ${response.statusText}`
        );
      }
      return response.json();
    })
    .then(jsonData => {
      return jsonData;
    });
}

function ReturnPlot(props) {
  const isDarkMode = useColorModeValue(false, true);
  var pf_returns = {
    y: props.return_data['pf_returns'].map(item => item.return),
    x: props.return_data['pf_returns'].map(item => item.date),
    type: 'scatter',
    mode: 'line',
    name: 'Portfolio returns',
    color: 'red',
  };
  var index_returns = {
    y: props.return_data['index_returns'].map(item => item.return),
    x: props.return_data['index_returns'].map(item => item.date),
    type: 'scatter',
    mode: 'line',
    name: 'Index returns',
    color: 'blue',
  };
  return (
    <Plot
      data={[pf_returns, index_returns]}
      layout={{
        title: 'Portfolio vs Index Returns',
        template: isDarkMode ? template_dark : {},
        margin: {
          l: 5,
          r: 5,
        },
        yaxis: {
          tickformat: ',.0%',
          fixedrange: true,
          automargin: true,
        },
        legend: { yanchor: 'top', y: -0.1, x: 1, xanchor: 'right' },
        xaxis: {
          fixedrange: true,
          rangeselector: {
            font: { color: 'black' },
            buttons: [
              {
                count: 1,
                label: '1m',
                step: 'month',
                stepmode: 'backward',
              },
              {
                count: 6,
                label: '6m',
                step: 'month',
                stepmode: 'backward',
              },
              { step: 'all' },
            ],
          },
        },
      }}
      useResizeHandler={true}
      style={{ width: '100%', height: '80%' }}
      config={{ displayModeBar: false }}
    />
  );
}

class Returns extends React.Component {
  render() {
    if (!this.props.loaded) {
      return (
        <Spinner
          thickness="4px"
          speed="0.65s"
          emptyColor="gray.200"
          color="blue.500"
          size="xl"
        />
      );
    }
    if (this.props.error) {
      return <Text>{this.props.error_data}</Text>;
    }
    return (
      <Box width={['100wv', '100vw', '100vw', '80vw']}>
        <ReturnPlot return_data={this.props.return_data}> </ReturnPlot>
      </Box>
    );
  }
}

function Menu(props) {
  return (
    <Tabs align="center">
      <TabList>
        <Tab>Returns</Tab>
        <Tab>Holdings</Tab>
        <Tab>Parameters</Tab>
      </TabList>
      <TabPanels>
        <TabPanel>
          <Returns {...props} />
        </TabPanel>
        <TabPanel>
          <Text> Holdings </Text>
        </TabPanel>
        <TabPanel>
          <Text> Parameters </Text>
        </TabPanel>
      </TabPanels>
    </Tabs>
  );
}

class App extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      return_data: 'Loading',
      loaded: false,
      error: false,
      error_data: 'Unknown error',
    };
  }

  componentDidMount() {
    getAPIData('returns')
      .then(jsonData => this.setState({ return_data: jsonData, loaded: true }))
      .catch(err => {
        console.log(`error ${err}`);
        if (err) {
          this.setState({ error_data: String(err) });
        }
        this.setState({ error: true, loaded: true });
      });
  }

  render() {
    return (
      <ChakraProvider theme={theme}>
        <Box textAlign="center" fontSize="xl">
          <Grid minH="10vh" p={3}>
            <ColorModeSwitcher justifySelf="flex-end" color="dark" />
            <Menu {...this.state} />
            {/* <VStack spacing={8} >
              <Text>
                Edit <Code fontSize="xl">src/App.js</Code> and save to reload.
              </Text>
              <Link
                color="teal.500"
                href="https://chakra-ui.com"
                fontSize="2xl"
                target="_blank"
                rel="noopener noreferrer"
              >
                Learn Chakra
              </Link>
            </VStack> */}
          </Grid>
        </Box>
      </ChakraProvider>
    );
  }
}

export default App;
