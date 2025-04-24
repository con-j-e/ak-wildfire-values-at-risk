//VERSION=3
function setup() {
    return {
        input: ['B02', 'B03', 'B04'],
        output: {id: 'default', bands: 3},
        mosaicking: 'ORBIT'
    };
}
function evaluatePixel(samples) {
    if (samples.length > 0) {
        return [2.5 * samples[0].B04, 2.5 * samples[0].B03, 2.5 * samples[0].B02];
    }
}
function updateOutputMetadata(scenes, inputMetadata, outputMetadata) {
    outputMetadata.userData = {'orbits': scenes.orbits}
}